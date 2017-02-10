get.exp.data <- function(jdbc.login, jdbc.password, test_bucket_ids, start_day, end_day, num_splits = 50, ext_conditions = ""){
  library("digest")
  library("DBI")
  library("rJava")
  library("RJDBC")
  
  fix.field.types <- function(data){
    data$test_bucket_id <- as.factor(data$test_bucket_id)
    # data$split_id  <- as.factor(data$split_id)
    data$pp_do     <- as.logical(data$pp_do)
    data$pp_km     <- as.logical(data$pp_km)
    data$pp_market <- as.logical(data$pp_market)
    
    return(data)
  }
  
  cache.path <- "../../tmp/"
  # для отладки
  # test_bucket_ids = c(10, 20); start_day = '2017-01-01'; end_day = '2017-01-02'; num_splits = 50; ext_conditions = "foo"
  
  # Преобразовываем параметры в строки для запроса
  # sort - чтобы можно было в любом порядке писать test_bucket_id и это не влияло бы на хэш
  cmd_test_bucket_ids <- paste0("set test_bucket_ids = array(", paste(sort(test_bucket_ids), collapse = ", "), ")")
  cmd_start_day <- paste0("set start_day = '", start_day, "'")
  cmd_end_day <- paste0("set end_day = '", end_day, "'")
  cmd_num_splits <- paste0("set num_splits = ", num_splits)
  cmd_ext_conditions <- paste0("set ext_conditions = ", ext_conditions)
  
  all_params <- paste(cmd_test_bucket_ids, cmd_start_day, cmd_end_day, cmd_num_splits, cmd_ext_conditions, sep = "\n")
  md5_hash_of_params <- digest(all_params)
  filename <- paste0(md5_hash_of_params, ".csv")
  filepath <- paste0(cache.path, filename)
  
  if (filename %in% list.files(path = cache.path)){
    # просто считываем файл и возвращаем его
    writeLines(paste0("Читаем данные из кэша: ", filepath))
    return(fix.field.types(read.csv(file = filepath)))
  }
  
  # Если считать из файла не удалось, то загружаем из HIVE
  writeLines("Данные в кэше не обнаружены. Загружаем непосредственно из HIVE...")
  
  # Инициализация параметров
  .jinit()
  options(java.parameters = "-Xmx8g")
  
  hivelibs.path <- list.files(path = "/Library/Frameworks/R.framework/Resources/lib/hive-jdbc/",
                              pattern = "*.jar",
                              full.names = TRUE)
  
  # Добавляем JAR файлы библиотек в class path
  for(lib in hivelibs.path){
    .jaddClassPath(lib)
  }
  
  # Создаём подключение
  drv <- JDBC("org.apache.hive.jdbc.HiveDriver")
  conn <- dbConnect(drv, "jdbc:hive2://hive-ssl.marmot.hdp.yandex.net:10000/analyst;ssl=true",
                    jdbc.login, jdbc.password)
  
  # устанавливем переменные для запроса
  dbSendUpdate(conn, cmd_test_bucket_ids)
  dbSendUpdate(conn, cmd_start_day)
  dbSendUpdate(conn, cmd_end_day)
  dbSendUpdate(conn, cmd_num_splits)
  dbSendUpdate(conn, cmd_ext_conditions)
  
  # читаем запрос
  fileName <- "../../src/SQL/sub.sql"
  query <- readChar(fileName, file.info(fileName)$size)
  
  # Выполняем запрос
  # result <- dbGetQuery(conn, "SELECT vhost, status, yandexuid FROM robot_market_logs.front_access LIMIT 10")
  result <- dbGetQuery(conn, query)
  
  # Закрываем соединение
  dbDisconnect(conn)
  
  # Сохраняем файл и информацию о параметрах
  write.csv(x = result, file = filepath, row.names = FALSE)
  
  fileConn<-file(paste0(filepath, ".details"))
  writeLines(all_params, fileConn)
  close(fileConn)
  
  # write.csv(x = all_params, file = , row.names = FALSE)
  writeLines(paste0("Данные из HIVE успешно загружены, а также сохранены в CSV файл: ", filepath))
  
  return(fix.field.types(result))
}