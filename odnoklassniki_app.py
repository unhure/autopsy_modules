# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os
from textwrap import wrap


def odnoklassniki(self, progressBar, odnoklassniki_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_odnoclassniki = case.getSleuthkitCase().addArtifactType("TSK_CHATS_OK", "Одноклассники - сообщения".decode('UTF-8'))
    except:
        artID_odnoclassniki = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_OK")
    try:
        artID_contacts_odnoclassniki = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CONTACTS_ODNOCLASSNIKI", "Одноклассники - контакты".decode('UTF-8'))
    except:
        artID_contacts_odnoclassniki = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_ODNOCLASSNIKI")

    try:
        attID_nr = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_ID", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Идентификатор сообщения".decode('UTF-8'))
    except:
        attID_nr = case.getSleuthkitCase().getAttributeType("TSK_MESS_ID")
    try:
        attID_sender = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_OTPRAV", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, отправивший сообщение".decode('UTF-8'))
    except:
        attID_sender = case.getSleuthkitCase().getAttributeType("TSK_MESS_OTPRAV")
    try:
        attID_reciever = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_RECIEVER", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, получивший сообщение".decode('UTF-8'))
    except:
        attID_reciever = case.getSleuthkitCase().getAttributeType("TSK_MESS_RECIEVER")
    try:
        attID_status = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_STATUS", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Дополнительная информация".decode('UTF-8'))
    except:
        attID_status = case.getSleuthkitCase().getAttributeType("TSK_MESS_STATUS")
    try:
        attID_companion = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_COMPANION", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Собеседник".decode('UTF-8'))
    except:
        attID_companion = case.getSleuthkitCase().getAttributeType("TSK_MESS_COMPANION")

    for file in odnoklassniki_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        # contacts
        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
            stmt2 = dbConn.createStatement()
            stmt3 = dbConn.createStatement()
            stmt4 = dbConn.createStatement()
            stmt5 = dbConn.createStatement()
        except SQLException as e:
            self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

        try:
            resultSet_account_info = stmt.executeQuery("select [uid],[login],[first_name],[last_name],[uri_pic] from authorized_users order by authorized_users.[uid]".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for odnoclassniki (" + e.getMessage() + ") account_info")

        try:
            resultSet_contacts = stmt2.executeQuery("select users.[user_id],users.[user_first_name],users.[user_last_name],users.[user_name],users.[user_avatar_url] from users order by users.[user_id]".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for odnoclassniki (" + e.getMessage() + ") contacts")

        if 'resultSet_account_info' in locals():
            while resultSet_account_info.next():
                try:
                    user_id = resultSet_account_info.getString("uid")
                    first_name = resultSet_account_info.getString("first_name")
                    last_name = resultSet_account_info.getString("last_name")
                    username = resultSet_account_info.getString("login")
                    pic = resultSet_account_info.getString("uri_pic")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (odnoclassniki) (" + e.getMessage() + ")")

                art = file.newArtifact(artID_contacts_odnoclassniki)

                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, first_name+" ".decode('UTF-8')+last_name))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, pic))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, "Логин: ".decode('UTF-8')+username))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, "Авторизированный пользователь приложения \"Одноклассники\"".decode('UTF-8')))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        if 'resultSet_contacts' in locals():
            while resultSet_contacts.next():
                try:
                    user_id = resultSet_contacts.getString("user_id")
                    first_name = resultSet_contacts.getString("user_first_name")
                    last_name = resultSet_contacts.getString("user_last_name")
                    user_name = resultSet_contacts.getString("user_name")
                    pic = resultSet_contacts.getString("user_avatar_url")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (odnoclassniki) (" + e.getMessage() + ")")

                art = file.newArtifact(artID_contacts_odnoclassniki)
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, first_name+" ".decode('UTF-8')+last_name))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user_name))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, pic))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                        IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                            IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        # messages
        try:
            columnFound = False
            metadata = dbConn.getMetaData()
            columnListResultSet = metadata.getColumns(None, None, "messages", None)
            while columnListResultSet.next():
                if columnListResultSet.getString("COLUMN_NAME") == "message":
                    columnFound = True
                    break
            if columnFound:
                try:
                    resultSet_messages = stmt3.executeQuery("select messages.[_id], messages.[author_id], (select users.[name] from users where messages.author_id=users._id) as user_name, messages.[message], messages.[_date] as date from messages order by messages._date".decode('UTF-8'))
                except SQLException as e:
                    self.log(Level.INFO, "Error querying database for odnoclassniki (messages-with-column-messages) (" + e.getMessage() + ")")

                try:
                    resultSet_messages = stmt5.executeQuery("select messages.[_id], messages.[author_id], (select users.[user_name] from users where messages.author_id=users.user_id) as user_name, messages.[message], messages.[_date] as date from messages order by messages._date".decode('UTF-8'))
                except SQLException as e:
                    self.log(Level.INFO, "Error querying database for odnoclassniki (messages-with-column-messages-2) (" + e.getMessage() + ")")
            else:
                try:
                    check_table = stmt3.executeQuery("SELECT name FROM sqlite_master where name like '%conversations%'".decode('UTF-8'))
                except SQLException as e:
                    self.log(Level.INFO, "Error querying database for odnoclassniki (check table) (" + e.getMessage() + ")")

                try:
                    conversation_table = check_table.getString("name")
                except:
                    self.log(Level.INFO, "Error to get data from tables info")
                    conversation_table = None

                if conversation_table is not None:
                    try:
                        tableFound = True
                        resultSet_messages = stmt4.executeQuery("select messages.[_id], messages.[_date], hex(messages.[data]) as message_hex_str, (select hex(conversations.data) from conversations where conversations.server_id=messages.conversation_id) as companion_hex_data from messages order by messages._date".decode('UTF-8'))
                    except SQLException as e:
                        self.log(Level.INFO, "Error querying database for odnoclassniki (with conversation table) (" + e.getMessage() + ")")
                else:
                    try:
                        resultSet_messages = stmt4.executeQuery("select messages.[_id], messages.[_date], hex(messages.[data]) as message_hex_str from messages order by messages._date".decode('UTF-8'))
                    except SQLException as e:
                        self.log(Level.INFO, "Error querying database for odnoclassniki (no conversation table) (" + e.getMessage() + ")")
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for odnoclassniki (messages) (" + e.getMessage() + ")")

        if 'resultSet_messages' in locals():
            if columnFound:
                while resultSet_messages.next():
                    try:
                        messages_id = resultSet_messages.getString("_id")
                        user_id = resultSet_messages.getString("author_id")
                        user_name = resultSet_messages.getString("user_name")
                        message = resultSet_messages.getString("message")
                        date = int(resultSet_messages.getString("date"))/1000
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from messages table (odnoclassniki) (" + e.getMessage() + ")")
                    art = file.newArtifact(artID_odnoclassniki)
                    art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, messages_id))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                        imdbIngestModuleFactory.moduleName, date))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                        imdbIngestModuleFactory.moduleName, message))
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, user_name+" (Идентификатор пользователя:   ".decode('UTF-8')+user_id+")".decode('UTF-8')))

                    IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                        IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                            IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            else:
                while resultSet_messages.next():
                    try:
                        messages_id = resultSet_messages.getString("_id")
                        message_hex_str = resultSet_messages.getString("message_hex_str")
                        date = int(resultSet_messages.getString("_date"))/1000
                        companion_hex_str = resultSet_messages.getString("companion_hex_data")
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from messages table (odnoclassniki) (" + e.getMessage() + ")")

                    if message_hex_str[0:2] == "0A":
                        message_length = int(message_hex_str[2:4], 16)
                        start_message = 4
                        array_to_find = wrap(message_hex_str, 2)
                        start_point_next = array_to_find.index("12")
                        end_message = start_point_next*2
                        message = message_hex_str[start_message:end_message]
                        message = bytearray.fromhex(message).decode('utf-8')
                        full_length = len(message_hex_str)/2

                        if message_length > full_length:
                            array_to_find = wrap(message_hex_str, 2)
                            start_point_id = array_to_find.index("12")*2+4
                            start_point_user = array_to_find.index("180148")*2+4
                        else:
                            string_to_find = message_hex_str[message_length*2+4:]
                            array_to_find = wrap(string_to_find, 2)
                            start_point_id = array_to_find.index("12")*2+message_length*2+4+4
                            start_point_user_test = string_to_find.find("E0AE90")

                        end_point_id = start_point_id+(int(message_hex_str[(start_point_id-2):(start_point_id)], 16))*2
                        end_point_user = message_hex_str.rfind("E0AE90")
                        user_id = (message_hex_str[start_point_id:end_point_id])
                        user_id = bytearray.fromhex(user_id).decode('utf-8')
                        if start_point_user_test == -1:
                            user_name = " ".decode('utf-8')
                        else:
                            start_point_user = start_point_user_test+message_length*2+4+6
                            user_name = (message_hex_str[start_point_user:end_point_user])
                            user_name = bytearray.fromhex(user_name).decode('utf-8')
                    else:
                        start_point_id = 4
                        end_point_id = start_point_id+(int(message_hex_str[(start_point_id-2):(start_point_id)], 16))*2
                        user_id = (message_hex_str[start_point_id:end_point_id])
                        user_id = bytearray.fromhex(user_id).decode('utf-8')
                        user_name = " ".decode('utf-8')

                    if tableFound:
                        conv_id_length = int(companion_hex_str[2:4], 16)
                        string_to_find = companion_hex_str[conv_id_length*2+4:]
                        array_to_find = wrap(string_to_find, 2)
                        start_point_name = array_to_find.index("1A")*2+conv_id_length*2+8
                        start_point_id = array_to_find.index("0A")*2+conv_id_length*2+8
                        companion_length = int(companion_hex_str[start_point_name-2:start_point_name], 16)
                        id_length = int(companion_hex_str[start_point_id-2:start_point_id], 16)
                        end_point_name = start_point_name+companion_length*2
                        end_point_id = start_point_id+id_length*2
                        companion = companion_hex_str[start_point_name:end_point_name]
                        companion = bytearray.fromhex(companion).decode('utf-8')
                        companion_id = companion_hex_str[start_point_id:end_point_id]
                        companion_id = bytearray.fromhex(companion_id).decode('utf-8')

                    art = file.newArtifact(artID_odnoclassniki)

                    art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, messages_id))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                        imdbIngestModuleFactory.moduleName, date))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                        imdbIngestModuleFactory.moduleName, message))
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, user_name+" (Идентификатор пользователя:   ".decode('UTF-8')+user_id+")".decode('UTF-8')))
                    if 'tableFound' in locals():
                        art.addAttribute(blackboardAttribute(attID_companion, imdbIngestModuleFactory.moduleName, companion+" (Идентификатор пользователя:   ".decode('UTF-8')+companion_id+")".decode('UTF-8')))

                    IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                        IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                            IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        IM_sqlitedb_android.IMDbIngestModule.set_count(self, 1)
        progressBar.progress(IM_sqlitedb_android.IMDbIngestModule.get_count(self))
        if odnoklassniki_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных: Одноклассники".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Одноклассники")

        try:
            if 'resultSet_account_info' in locals():
                resultSet_account_info.close()
            if 'resultSet_contacts' in locals():
                resultSet_contacts.close()
            if 'resultSet_messages' in locals():
                resultSet_messages.close()
            stmt.close()
            stmt2.close()
            stmt3.close()
            stmt4.close()
            stmt5.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
