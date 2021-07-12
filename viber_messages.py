# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os


def viber_messages(self, progressBar, viber_messages_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_viber1 = case.getSleuthkitCase().addArtifactType("TSK_CHATS_VIBER1", "Viber - сообщения".decode('UTF-8'))
    except:
        artID_viber1 = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VIBER1")

    try:
        attID_sender = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_OTPRAV", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, отправивший сообщение".decode('UTF-8'))
    except:
        attID_sender = case.getSleuthkitCase().getAttributeType("TSK_MESS_OTPRAV")
        
    try:
        attID_reciever = case.getSleuthkitCase().addArtifactAttributeType("TSK_MESS_RECIEVER", blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, получивший сообщение".decode('UTF-8'))
    except:
        attID_reciever = case.getSleuthkitCase().getAttributeType("TSK_MESS_RECIEVER")

        
    for file in viber_messages_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
            stmt1 = dbConn.createStatement()
        except SQLException as e:
            self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

        try:
            check_table = stmt.executeQuery("SELECT name FROM sqlite_master where name like '%PARTICIPANTS%_member_id_%'")
            if check_table.getString("name") is not None:
                base_version = 1
        except SQLException as e:
            self.log(Level.INFO, "Error check viber table (" + e.getMessage() + ")")
            base_version = 0

        if base_version == 0:
            try:
                resultSet = stmt.executeQuery("select case type when 0 then (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) when 1 then (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) || ' (' || 'Аккаунт мобильного телефона)' end as name, (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info where messages.address=participants_info.[number])  as name2, (select display_name || ' ' || number || ' (Аккаунт мобильного телефона)' from participants_info where participant_type=0) as account, messages.type as type, case when messages.body='' or body is NULL then extra_mime else messages.body end as text, messages.date as date from messages ORDER BY messages.date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for viber messages table (" + e.getMessage() + ")")
        else:
            try:
                resultSet = stmt.executeQuery("select case type when 0 then (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) || ' (' || messages.address || ')' when 1 then (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) || ' (' || 'Аккаунт мобильного телефона)' end as name, (select display_name || ' (' || number || ')' from messages, participants_info where messages.address=participants_info.[member_id]) as name2, (select display_name || ' (Аккаунт мобильного телефона)' from participants_info where participant_type=0) as account, messages.type as type, case when messages.body='' or body is NULL then extra_mime else messages.body end as text, messages.date as date from messages ORDER BY messages.date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for viber messages table 1 (" + e.getMessage() + ")")
            try:
                resultSet = stmt1.executeQuery("select case send_type when 0 then (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) || ' (' || messages.user_id || ')' when 1 then (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) || ' (' || 'Аккаунт мобильного телефона)' end as name, (select display_name || ' (' || number || ')' from messages, participants_info where messages.user_id=participants_info.[member_id]) as name2, (select display_name || ' (Аккаунт мобильного телефона)' from participants_info where participant_type=0) as account, messages.send_type as type, case when messages.body='' or body is NULL then extra_mime else messages.body end as text, messages.msg_date as date from messages ORDER BY messages.msg_date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for viber messages table 2(" + e.getMessage() + ")")

        if 'resultSet' in locals():
            while resultSet.next():
                try:
                    contact = resultSet.getString("name")
                    contact2 = resultSet.getString("name2")
                    account = resultSet.getString("account")
                    mess_type = resultSet.getInt("type")
                    date = int(resultSet.getString("date"))/1000
                    text = resultSet.getString("text")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from viber messages table (" + e.getMessage() + ")")

                art = file.newArtifact(artID_viber1)  

                if mess_type == 0:
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, contact))
                    art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, account))
                elif mess_type == 1:
                    art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, contact2))
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, contact))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, text))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        file_count = IM_sqlitedb_android.IMDbIngestModule.get_count(self) + 1
        IM_sqlitedb_android.IMDbIngestModule.set_count(self, file_count)
        progressBar.progress(file_count)
        if viber_messages_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных: Viber (сообщения)".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Viber (сообщения)")

        try:
            if 'resultSet' in locals():
                resultSet.close()
            stmt.close()
            stmt1.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
