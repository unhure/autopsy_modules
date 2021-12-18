# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os


def skype(self, progressBar, skype_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_contacts_skype = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CONTACTS_SKYPE", "Skype - контакты".decode('UTF-8'))
    except:
        artID_contacts_skype = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_SKYPE")
    try:
        artID_calllog_skype = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CALLLOG_SKYPE", "Skype - журнал звонков".decode('UTF-8'))
    except:
        artID_calllog_skype = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CALLLOG_SKYPE")
    try:
        artID_skype = case.getSleuthkitCase().addArtifactType("TSK_CHATS_SKYPE", "Skype - сообщения".decode('UTF-8'))
    except:
        artID_skype = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_SKYPE")

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
        
    for file in skype_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
            stmt1 = dbConn.createStatement()
            stmt2 = dbConn.createStatement()
            stmt3 = dbConn.createStatement()
            stmt4 = dbConn.createStatement()
            stmt5 = dbConn.createStatement()
            stmt6 = dbConn.createStatement()
            stmt7 = dbConn.createStatement()
        except SQLException as e:
            self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

        try:
            resultSet_messages = stmt.executeQuery("select Messages.[id], Messages.[timestamp], Messages.[dialog_partner], Messages.[author], Messages.[body_xml], Messages.[reason], Messages.[convo_id], Messages.[type] from Messages where Messages.[chatmsg_type]=3 order by Messages.[timestamp]".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-resultSet_messages 1 (" + e.getMessage() + ")")

        try:
            resultSet_messages = stmt1.executeQuery("select chatItem.client_message_id as [id], chatItem.time/1000 as [timestamp], case chatItem.is_sender_me when 0 then NULL else chatItem.conversation_link end as [dialog_partner], case chatItem.is_sender_me when 1 then (select user.first_name || ' ' || user.last_name || '(' || user.username || ')' from user) else chatItem.conversation_link end as [author], chatItem.content as [body_xml], chatItem.call_failure_reason as [reason], '-' as [convo_id], chatItem.message_type as [type] from chatItem where chatItem.message_type!=3 order by chatItem.time".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-resultSet_messages 2 (" + e.getMessage() + ")")

        try:
            account_info = stmt2.executeQuery("select Accounts.[fullname], Accounts.[skypename], Accounts.[emails], Accounts.[phone_home], Accounts.[phone_mobile] from Accounts".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-account_info 1 (" + e.getMessage() + ")")

        try:
            account_info = stmt3.executeQuery("select user.first_name || ' ' || user.last_name || '(' || user.username || ')' as fullname, user.skype_name as skypename, '-' as emails, user.phone_numbers as phone_home, '-' as phone_mobile from user".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-account_info 2 (" + e.getMessage() + ")")

        try:
            resultSet_contacts = stmt4.executeQuery("select Contacts.[fullname], Contacts.[skypename],  Contacts.[birthday], Contacts.[phone_home], Contacts.[phone_office],  Contacts.[phone_mobile], Contacts.[emails], Contacts.[homepage], Contacts.[about] from Contacts".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-resultSet_contacts 1 (" + e.getMessage() + ")")

        try:
            resultSet_contacts = stmt5.executeQuery("select person.first_name || ' ' || person.last_name as fullname, case when person.skype_name is NULL then NULL else person.skype_name end as [skypename], case person.[birthday] when -1 then NULL else strftime('%Y-%m-%d', person.[birthday]/1000, 'unixepoch','+3 hours') end as [birthday], person.phone_numbers as [phone_home], '-' as [phone_office], '-' as [phone_mobile], '-' as [emails], '-' as [homepage], '-' as [about] from person".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-resultSet_contacts 2 (" + e.getMessage() + ")")

        try:
            resultSet_calls = stmt6.executeQuery("select Calls.[id], CallMembers.[type], Calls.[host_identity], CallMembers.[identity],  CallMembers.[dispname] , Calls.[begin_timestamp], Calls.[duration],  CallMembers.[guid] from Calls, CallMembers where Calls.[id]=CallMembers.[call_db_id]".decode('UTF-8'))                
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-resultSet_calls 1 (" + e.getMessage() + ")")

        try:
            resultSet_calls = stmt7.executeQuery("select chatItem.client_message_id as id, case chatItem.is_sender_me when 1 then 2 when 0 then 1 end as [type], '-' as [host_identity], chatItem.conversation_link as [identity],  (select (case when person.first_name is NULL then '' else person.first_name end) || ' ' || (case when person.last_name is NULL then '' else person.last_name end) || ' (' || person.skype_name || ')' from person where chatItem.conversation_link=person.entry_id) as [dispname] , chatItem.time/1000 as [begin_timestamp], chatItem.duration/1000 as [duration],  '-' as [guid] from chatItem where chatItem.message_type=3".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for skype-resultSet_calls 2 (" + e.getMessage() + ")")

        account_tmp = []
        if 'account_info' in locals():
            try:
                if account_info.getString("fullname"):
                    account_tmp.append(account_info.getString("fullname"))
                    account_tmp.append(" Логин: ".decode('UTF-8'))
                    account_tmp.append(account_info.getString("skypename"))
                user_email = account_info.getString("emails")
                if user_email != "-":
                    account_tmp.append(" (".decode('UTF-8'))
                    account_tmp.append(user_email)
                    account_tmp.append(")".decode('UTF-8'))
                    account = ''.join(account_tmp)
                else:
                    account_tmp.append(account_info.getString("skypename"))
                    account_tmp.append(" (".decode('UTF-8'))
                    account_tmp.append(account_info.getString("emails"))
                    account_tmp.append(")".decode('UTF-8'))
                    account = ''.join(account_tmp)
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for account table (skype) (" + e.getMessage() + ")")

        if 'resultSet_messages' in locals():
            while resultSet_messages.next():
                try:
                    mess_id = resultSet_messages.getString("id")
                    dialog_partner = resultSet_messages.getString("dialog_partner")
                    author = resultSet_messages.getString("author")
                    date = int(resultSet_messages.getString("timestamp"))
                    text = resultSet_messages.getString("body_xml")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from messages table (skype) (" + e.getMessage() + ")")

                art = file.newArtifact(artID_skype)
                art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, mess_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date))

                if dialog_partner is None:
                    art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, account))
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, author))
                else:
                    art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, dialog_partner))
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, account))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, text))

        if 'resultSet_contacts' in locals():
            while resultSet_contacts.next():
                try:
                    skypename = resultSet_contacts.getString("skypename")
                    fullname = resultSet_contacts.getString("fullname")
                    if fullname is None:
                        fullname = resultSet_contacts.getString("skypename")
                    else:
                        tmp = []
                        tmp.append(resultSet_contacts.getString("fullname"))
                    if skypename:
                        tmp.append(" (Логин: ".decode('UTF-8'))
                        tmp.append(resultSet_contacts.getString("skypename"))
                        tmp.append(")".decode('UTF-8'))
                        fullname = "".join(tmp)
                    home_phone = resultSet_contacts.getString("phone_home")
                    phone_office = resultSet_contacts.getString("phone_office")
                    phone_mobile = resultSet_contacts.getString("phone_mobile")
                    email = resultSet_contacts.getString("emails")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (skype) (" + e.getMessage() + ")")

                art = file.newArtifact(artID_contacts_skype)
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, fullname))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_HOME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, home_phone))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_OFFICE.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, phone_office))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_MOBILE.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, phone_mobile))

        if 'resultSet_calls' in locals():
            while resultSet_calls.next():
                try:
                    date_begin = int(resultSet_calls.getString("begin_timestamp"))
                    # self.log(Level.INFO, "date_begin = " + str(date_begin))
                    date_end = date_begin+resultSet_calls.getInt("duration")
                    contact = resultSet_calls.getString("identity")
                    dispname = resultSet_calls.getString("dispname")
                    call_type = resultSet_calls.getInt("type")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from calls table (skype) (" + e.getMessage() + ")")

                art = file.newArtifact(artID_calllog_skype)
                tmp = []
                if dispname:
                    tmp.append(dispname)
                if contact:
                    tmp.append(" (".decode('UTF-8'))
                    tmp.append(contact)
                    tmp.append(")".decode('UTF-8'))
                fullname = "".join(tmp)
                if call_type == 1:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, fullname))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, account))
                elif call_type == 2:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, fullname))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, account))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_START.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, date_begin))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_END.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, date_end))
                if call_type == 1:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DIRECTION.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, "Входящий".decode('UTF-8')))
                elif call_type == 2:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DIRECTION.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, "Исходящий".decode('UTF-8')))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        IM_sqlitedb_android.IMDbIngestModule.set_count(self, 1)
        progressBar.progress(IM_sqlitedb_android.IMDbIngestModule.get_count(self))
        if skype_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных: Skype".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Skype")

        try:
            if 'resultSet_messages' in locals():
                resultSet_messages.close()
            if 'account_info' in locals():
                account_info.close()
            if 'resultSet_contacts' in locals():
                resultSet_contacts.close()
            if 'resultSet_calls' in locals():
                resultSet_calls.close()
            stmt.close()
            stmt1.close()
            stmt2.close()
            stmt3.close()
            stmt4.close()
            stmt5.close()
            stmt6.close()
            stmt7.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
