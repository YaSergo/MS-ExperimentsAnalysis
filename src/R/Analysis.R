library(dplyr)
library(ggplot2)

source("GetDataFromHIVE.R")

num.splits <- 25 # 100, 50, 25, 20, ...
p.value.threshold <- 0.05 # порог для стат. значимости

if (!file.exists("./credentials.RData")){
  stop("Создайте файл credentials.RData c вашим логином и паролем для подключения к HIVE")
}
load(file = "./credentials.RData")

# Для создания файла credentials.RData  раскомментируйте строки ниже:
# credentials          <- list()
# credentials$login    <- "your.login"
# credentials$password <- "your.password"
# save(credentials, file = "./credentials.RData")

params4check <- c(
  "cpc_clicks_num",
  "cpa_clicks_num",
  "cpc_clicks_price",
  "cpa_order_is_billed_num",
  "cpa_order_is_billed_revenue",
  "all_clicks_num",
  "all_money"
)

pplevels <- c(
  "pp_do",
  "pp_km",
  "pp_market"
)

exp.analysis <- function(test_ids, control_ids, start_day, end_day, num_splits = 50, ext_conditions = ""){
  test_bucket_ids <- c(test_ids, control_ids)
  
  experiment.data <- get.exp.data(jdbc.login = credentials$login,
                                  jdbc.password = credentials$password,
                                  test_bucket_ids = test_bucket_ids,
                                  start_day = start_day,
                                  end_day = end_day,
                                  num_splits = num.splits,
                                  ext_conditions = ext_conditions)
  result.list <- list()
  for (pplevel in pplevels){
    #print(pplevel)
    
    tmp.data <- experiment.data %>%
      filter_(paste0(pplevel, " == TRUE")) %>%
      group_by(test_bucket_id, split_id) %>%
      summarise(cpc_clicks_num = sum(cpc_clicks_num),
                cpa_clicks_num = sum(cpa_clicks_num),
                cpc_clicks_price = sum(cpc_clicks_price),
                cpa_order_is_billed_num = sum(cpa_order_is_billed_num),
                cpa_order_is_billed_revenue = sum(cpa_order_is_billed_revenue),
                all_clicks_num = sum(all_clicks_num),
                all_money = sum(all_money)) %>%
      ungroup() %>%
      as.data.frame()
    
    
    for (control_id in control_ids){
      for (test_id in test_ids){
        for (param in params4check){
          
          control.vector <- subset(x = tmp.data,
                                   subset = test_bucket_id == control_id)[, param]
          test.vector    <- subset(x = tmp.data,
                                   subset = test_bucket_id == test_id)[, param]
          result.list[[length(result.list) + 1]] <- data.frame(
            pplevel = pplevel,
            control_id = control_id,
            test_id = test_id,
            param = param,
            control_mean = round(mean(control.vector), 2),
            test_mean = round(mean(test.vector), 2),
            delta = round(mean(test.vector) - mean(control.vector), 2),
            rate = round(mean(test.vector) / mean(control.vector) - 1, 3),
            p.value = round(t.test(x = test.vector, y = control.vector)$p.value, 3)
          )
          
        }
      }
    }
    result <- Reduce(f = rbind, x = result.list)
    result$stat.significant <- ifelse(result$p.value < p.value.threshold, TRUE, FALSE)
  }
  
  return(result)
}

# MA-1968
ma1968.result <- exp.analysis(test_ids = c(36208, 36207), control_ids = c(36206),
                              start_day = "2016-12-16", end_day = "2017-01-08",
                              num_splits = num.splits, ext_conditions = "AND geo_id = 213")

# MA-1908
ma1908.result <- exp.analysis(test_ids = c(35250), control_ids = c(35251),
                              start_day = "2016-12-01", end_day = "2016-12-07",
                              num_splits = num.splits)
