# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os
from textwrap import wrap
from pickle import FALSE


def rugram(self, progressBar, rugram_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_rugram = case.getSleuthkitCase().addArtifactType(
            "TSK_CHATS_RUGRAM", "RUGRAM - сообщения".decode("UTF-8")
        )
    except:
        artID_rugram = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_RUGRAM")

    try:
        artID_contacts_rugram = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CONTACTS_RUGRAM", "RuGram - контакты".decode('UTF-8'))
    except:
        artID_contacts_rugram = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_RUGRAM")
        
    try:
        attID_nr = case.getSleuthkitCase().addArtifactAttributeType(
            "TSK_MESS_ID",
            blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING,
            "Идентификатор сообщения".decode("UTF-8"),
        )
    except:
        attID_nr = case.getSleuthkitCase().getAttributeType("TSK_MESS_ID")
    
    try:
        attID_status = case.getSleuthkitCase().addArtifactAttributeType(
            "TSK_MESS_STATUS",
            blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING,
            "Дополнительная информация".decode("UTF-8"),
        )
    except:
        attID_status = case.getSleuthkitCase().getAttributeType("TSK_MESS_STATUS")

    try:
        attID_companion = case.getSleuthkitCase().addArtifactAttributeType(
            "TSK_MESS_COMPANION",
            blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING,
            "Собеседник/Группа".decode("UTF-8"),
        )
    except:
        attID_companion = case.getSleuthkitCase().getAttributeType("TSK_MESS_COMPANION")

    for file in rugram_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
        except SQLException as e:
            self.log(
                Level.INFO,
                "Could not open database file (not SQLite) "
                +file.getName()
                +" ("
                +e.getMessage()
                +")",
            )

        try:
            resultSet_messages = stmt.executeQuery(
                "SELECT messages.[mid], messages.[uid], messages.[send_state],\
                case when messages.[uid] < 0 then \
                    (select name from chats where messages.[uid]*(-1)=chats.[uid]) \
                else \
                    (select name from users where messages.[uid]=users.[uid])\
                end as [name],\
                messages.[read_state], messages.[date], hex(messages.[data]) as [data] \
                from messages \
                order by messages.[date]".decode("UTF-8")
            )
        except SQLException as e:
            self.log(
                Level.INFO,
                "Error querying database for RuGram (" + e.getMessage() + ")",
            )

        message_number = 0
        
        if "resultSet_messages" in locals():
            while resultSet_messages.next():
                try:
                    mess_id = resultSet_messages.getString("mid")
                    user = (
                        resultSet_messages.getString("name")
                        +"("
                        +resultSet_messages.getString("uid")
                        +")"
                    )
                    send_state = resultSet_messages.getString("send_state")
                    read_state = resultSet_messages.getInt("read_state")
                    date = resultSet_messages.getInt("date")
                    text_hex_string = resultSet_messages.getString("data")
                except SQLException as e:
                    self.log(
                        Level.INFO,
                        "Error getting values from messages table (RuGram) ("
                        +e.getMessage()
                        +")",
                    )
                
                message_number += 1 
                # 32E5DDBD начало сообщения от групп?
                # 6DBCB19D начало сообщения от пользователей?
                # далее 4 байт ID собеседника и 4 байт даты (unix time);
                start_text = text_hex_string.rfind("32E5DDBD") + 8 * 3
                if start_text == 23:
                    start_text = text_hex_string.rfind("6DBCB19D") + 8 * 3

                end_text = text_hex_string.rfind("D7505169")
                
                if text_hex_string[start_text:start_text + 2] == "00":
                    media_content = True
                    text = ""
                else:
                    media_content = False
                
                # Размер текста в 2 байтах после 4 байт: ID собеседника 
                # и 4 байт даты (unix time) и после последовательности "FE" (254);    
                start_point_temp = text_hex_string.find("FE", start_text, end_text)
                if start_point_temp % 2 == 0:
                    start_point = start_point_temp
                    # меняем Endian и получаем число указывающее количество байт текста
                    text_size = int(
                            text_hex_string[start_point + 4: start_point + 6]
                            +text_hex_string[start_point + 2: start_point + 4],
                            16,
                            )
                    if text_size * 2 > len(text_hex_string[start_point + 8:len(text_hex_string)]) * 2:
                        start_point = -1
                        base_txt = 2
                    else:
                        base_txt = int(
                            text_hex_string[start_point:start_point + 2], 16)                          
                else:
                    start_point = -1
                    base_txt = 2              
                                                               
                if base_txt == 254:
                    if not media_content:
                        # размер текста умножаем на 2 поскольку в байте 2 позиции hex
                        text = bytearray.fromhex(
                            text_hex_string[start_point + 8: start_point + 8 + text_size * 2]
                            ).decode("utf-8", "replace")
                            
                        self.log(
                            Level.INFO,
                            "*************** End message: " + str(message_number),
                        )
                else:
                    # Размер текста в 1 байте после 4 байт: ID собеседника и 4 байт даты (unix time);                   
                    text_size = int(
                        text_hex_string[start_text:start_text + 2], 16
                        )
                    # размер текста умножаем на 2 поскольку в байте 2 позиции hex
                    try:
                        text = bytearray.fromhex(
                            text_hex_string[start_text + 2: start_text + 2 + text_size * 2]
                            ).decode("utf-8", "replace")
                        self.log(
                            Level.INFO,
                            "*************** END Message 2: " + str(message_number),
                            )
                    except UnicodeDecodeError:
                        text = bytearray.fromhex(
                            text_hex_string[start_text + 2: len(text_hex_string)]
                            ).decode("utf-8", "replace")
                        self.log(
                                Level.INFO,
                                "*************** END Message 2-2: " + str(message_number),
                                )

                art = file.newArtifact(artID_rugram)
                art.addAttribute(
                    blackboardAttribute(
                        attID_nr, imdbIngestModuleFactory.moduleName, mess_id
                    )
                )
                art.addAttribute(
                    blackboardAttribute(
                        blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                        imdbIngestModuleFactory.moduleName,
                        date,
                    )
                )
                art.addAttribute(
                    blackboardAttribute(
                        blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                        imdbIngestModuleFactory.moduleName,
                        text,
                    )
                )
                art.addAttribute(
                    blackboardAttribute(
                        attID_companion, imdbIngestModuleFactory.moduleName, user
                    )
                )
                
                if read_state == 2:
                    read_state = "Не прочитано".decode("UTF-8")
                elif read_state == 3:
                    read_state = "Прочитано".decode("UTF-8")
                    
                if not media_content:
                    art.addAttribute(
                        blackboardAttribute(
                            attID_status,
                            imdbIngestModuleFactory.moduleName,
                            "Статус: ".decode("UTF-8") + read_state,
                        )
                    )
                else:
                    art.addAttribute(
                        blackboardAttribute(
                            attID_status,
                            imdbIngestModuleFactory.moduleName,
                            "Передан мультимедийный контент (изображение, видео, звуковой файл и т.п.)".decode("UTF-8") + \
                            "; Статус: ".decode("UTF-8") + read_state,
                        )
                    )
                    
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(
                        imdbIngestModuleFactory.moduleName,
                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE,
                        None,
                        )
                    )
        if "resultSet_messages" in locals():
            resultSet_messages.close()
        stmt.close()

        stmt = dbConn.createStatement()
        try:
            resultSet_contacts = stmt.executeQuery(
                "SELECT user_contacts_v7.uid,\
                user_contacts_v7.fname || ' ' || user_contacts_v7.sname as [name], \
                (select user_phones_v7.phone || ' (' || user_phones_v7.phone || ')' \
                from user_phones_v7 where user_contacts_v7.[key]=user_phones_v7.[key]) as [phones] \
                from user_contacts_v7".decode("UTF-8")
            )
        except SQLException as e:
            self.log(
                Level.INFO,
                "Error querying database for RuGram (contacts 1) (" + e.getMessage() + ")",
            )

        if "resultSet_contacts" in locals():
            while resultSet_contacts.next():
                try:
                    user_id = resultSet_contacts.getString("uid")
                    user_name = resultSet_contacts.getString("name")
                    phone_number = resultSet_contacts.getString("phones")
                except SQLException as e:
                    self.log(
                        Level.INFO,
                        "Error getting values from contacts table 1 (RuGram) ("
                        +e.getMessage()
                        +")",
                    )
                art = file.newArtifact(artID_contacts_rugram)
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_name))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, phone_number))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                        IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                            IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        if "resultSet_contacts" in locals():
            resultSet_contacts.close()
        stmt.close()
                
        stmt = dbConn.createStatement()
        try:
            resultSet_contacts = stmt.executeQuery(
                "SELECT users.uid, users.name, hex(users.[data]) as [data] \
                from users".decode("UTF-8")
            )
        except SQLException as e:
            self.log(
                Level.INFO,
                "Error querying database for RuGram (contacts 2) (" + e.getMessage() + ")",
            )
                
        if "resultSet_contacts" in locals():
            while resultSet_contacts.next():
                try:
                    user_id = resultSet_contacts.getString("uid")
                    user_name = resultSet_contacts.getString("name")
                    contacts_hex_string = resultSet_contacts.getString("data")
                except SQLException as e:
                    self.log(
                        Level.INFO,
                        "Error getting values from contacts table 2 (RuGram) ("
                        +e.getMessage()
                        +")",
                    )
                hex_array = wrap(contacts_hex_string, 2)
                st_count = hex_array.count("0C")
                leng = len(contacts_hex_string)
                finded_hex_string = ""
                phone_number = ""
                shift = 0
                
                for i in range(0,st_count):
                    test_string = contacts_hex_string[shift * 2:leng]
                    test_array = wrap(test_string, 2)
                    try:
                        phone_start_tmp = test_array.index("0C") + shift
                    except ValueError:
                        phone_start_tmp = -1
                    end_phone_tmp = test_string.find("000000", phone_start_tmp * 2, phone_start_tmp * 2 + 34)
                    if end_phone_tmp == -1:
                        shift = phone_start_tmp + 1
                        phone_start_tmp = -1
                    else:
                        phone_start = phone_start_tmp * 2
                        end_phone = end_phone_tmp + shift * 2
                        finded_hex_string = contacts_hex_string[phone_start + 2: end_phone]
                    if len(finded_hex_string) % 2 == 0:
                        phone_number = bytearray.fromhex(
                            finded_hex_string
                            ).decode("utf-8", "replace")
                        if phone_number.isdigit():
                            break
                        else:
                            phone_number = ""
                    else:
                        phone_number = ""    
                    
                art = file.newArtifact(artID_contacts_rugram)
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_name))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, phone_number))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                        IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                            IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
    
        try:
            if "resultSet_contacts" in locals():
                resultSet_contacts.close()
            stmt.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

        file_count = IM_sqlitedb_android.IMDbIngestModule.get_count(self) + 1
        IM_sqlitedb_android.IMDbIngestModule.set_count(self, file_count)
        progressBar.progress(file_count)
        if rugram_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(
                IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                imdbIngestModuleFactory.moduleName,
                "Обнаружены базы данных: RuGram".decode("UTF-8"),
            )
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "RuGram")
