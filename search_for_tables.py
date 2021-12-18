# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os
from textwrap import wrap


def search_for_tables(self, progressBar, all_db_files, list_Of_Tables):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    # try:
    #     artID_tables = case.getSleuthkitCase().addArtifactType("TSK_TABLES_NEED", "Интересуемые таблицы".decode('UTF-8'))
    # except:
    #     artID_tables = case.getSleuthkitCase().getArtifactTypeID("TSK_TABLES_NEED")

    try:
        attID_table_name = case.getSleuthkitCase().addArtifactAttributeType("TSK_TABLE_NAME", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Имя таблицы".decode('UTF-8'))
    except:
        attID_table_name = case.getSleuthkitCase().getAttributeType("TSK_TABLE_NAME")

    try:
        attID_row_sum = case.getSleuthkitCase().addArtifactAttributeType("TSK_ROW_SUM", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Количество строк в таблице".decode('UTF-8'))
    except:
        attID_row_sum = case.getSleuthkitCase().getAttributeType("TSK_ROW_SUM")

    max_elem_db = len(all_db_files)
    done_db = 0
    for file in all_db_files:
        meta_get = False
        for table in list_Of_Tables:
            # self.log(Level.INFO, "Processing file: " + file.getName())
            lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
            IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))
            column_name = []
            
            try:
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
                stmt = dbConn.createStatement()
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

            try:            
                metadata = dbConn.getMetaData()
                # self.log(Level.INFO, "Processing table: " + table)
                columnListResultSet = metadata.getTables(None, None, "%", None);
                while columnListResultSet.next():
                    real_table_name = columnListResultSet.getString(3)
                    if (table == real_table_name) or (table in real_table_name):
                        column_name.append(real_table_name)
                meta_get = True
            except SQLException as e:
                self.log(Level.INFO, "Could not get meta info from  " + file.getName() + " (" + e.getMessage() + ")")
            
            for col in column_name:   
                try:
                    row_count_query = stmt.executeQuery("SELECT COUNT(*) as SUM FROM ".decode('UTF-8') + col)
                except SQLException as e:
                    self.log(Level.INFO, "Error querying database for row count (" + e.getMessage() + ")")

                database_path = file.getUniquePath()
                row_sum = row_count_query.getString("SUM")
                # art = file.newArtifact(artID_tables)
                art = file.newArtifact(IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_INTERESTING_FILE_HIT) 
                    
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_SET_NAME.getTypeID(), 
                                                     imdbIngestModuleFactory.moduleName, "Интересуемые таблицы в базах данных".decode('UTF-8'))) 
                art.addAttribute(blackboardAttribute(attID_table_name, imdbIngestModuleFactory.moduleName, col))
                # art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PATH.getTypeID(),
                #                                     imdbIngestModuleFactory.moduleName, database_path))
                art.addAttribute(blackboardAttribute(attID_row_sum, imdbIngestModuleFactory.moduleName, row_sum))
                
        
        if meta_get:
            done_db += 1                        
        IM_sqlitedb_android.IMDbIngestModule.set_count(self, 1)
        progressBar.progress(IM_sqlitedb_android.IMDbIngestModule.get_count(self))
        if all_db_files.index(file) == (max_elem_db-1) and 'row_sum' in locals():
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.INFO,
                                                                      imdbIngestModuleFactory.moduleName, "Поиск таблиц осуществлен в ".decode('UTF-8') + str(done_db) + " базах данных из ".decode('UTF-8') + str(max_elem_db))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)

        try:
            if 'columnListResultSet' in locals():
                columnListResultSet.close()
            if 'row_count_query' in locals():
                row_count_query.close()
            stmt.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
