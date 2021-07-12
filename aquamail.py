# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os


def aquamail(self, progressBar, aquamail_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_aquamail = case.getSleuthkitCase().addArtifactType("TSK_CHATS_AQUAMAIL", "Aquamail - электронная почта".decode('UTF-8'))
    except:
        artID_aquamail = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_AQUAMAIL")

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

    for file in aquamail_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
        except SQLException as e:
            self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

        try:
            resultSet_messages = stmt.executeQuery("select message.[msg_id], message.[who_from], message.[who_to], message.[when_date], message.[subject],  message.[has_attachments], message.[preview_attachments], message.[body_alt_content_utf8], (select group_concat(stored_file_name, '; ') from (select part.[stored_file_name] from part where message.[_id]=part.[message_id])) as joinedAttachmentInfos from message order by message.[when_date]".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for aquamail (" + e.getMessage() + ")")

        if 'resultSet_messages' in locals():
            while resultSet_messages.next():
                try:
                    mess_id = resultSet_messages.getString("msg_id")
                    dialog_partner = resultSet_messages.getString("who_to")
                    author = resultSet_messages.getString("who_from")
                    date_sent = int(resultSet_messages.getString("when_date"))/1000
                    text = resultSet_messages.getString("body_alt_content_utf8")
                    subject = resultSet_messages.getString("subject")
                    preview_attachments=resultSet_messages.getString("preview_attachments")
                    attachment = resultSet_messages.getString("joinedAttachmentInfos")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from messages table (aquamail) (" + e.getMessage() + ")")

                status_arr = []
                if subject:
                    status_arr.append("Тема письма: \"".decode('UTF-8'))
                    status_arr.append(subject)
                    status_arr.append("\"; ".decode('UTF-8'))
                if preview_attachments:
                    status_arr.append("Вложения (имена файлов): \"".decode('UTF-8'))
                    status_arr.append(preview_attachments)
                if attachment:
                    status_arr.append("\"; ".decode('UTF-8'))
                    status_arr.append("Вложения (путь к файлам): \"".decode('UTF-8'))
                    status_arr.append(attachment)
                    status_arr.append("\"; ".decode('UTF-8'))
                status = ' '.join(status_arr)

                art = file.newArtifact(artID_aquamail)
                art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, mess_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_SENT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date_sent))
                art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, author))
                art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, dialog_partner))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, text))
                art.addAttribute(blackboardAttribute(attID_status, imdbIngestModuleFactory.moduleName, status))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        file_count = IM_sqlitedb_android.IMDbIngestModule.get_count(self) + 1
        IM_sqlitedb_android.IMDbIngestModule.set_count(self, file_count)
        progressBar.progress(file_count)
        if aquamail_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных: Aquamail".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Aquamail")
              
        try:
            if resultSet_messages in locals():
                resultSet_messages.close()
            stmt.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
