# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os


def gmail(self,  progressBar, gmail_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()
    
    try:
        artID_gmail = case.getSleuthkitCase().addArtifactType("TSK_CHATS_GMAIL", "Gmail - электронная почта".decode('UTF-8'))
    except:
        artID_gmail = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_GMAIL")

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

    for file in gmail_files:
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
            resultSet_messages = stmt.executeQuery("select messages.[messageId], messages.[fromAddress], messages.[toAddresses], messages.[dateSentMs], messages.[dateReceivedMs], messages.[subject],  messages.[snippet], messages.[body], messages.[joinedAttachmentInfos] from messages order by messages.[dateSentMs]".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for gmail (" + e.getMessage() + ")")

        if 'resultSet_messages' in locals():
            while resultSet_messages.next():
                try:
                    mess_id = resultSet_messages.getString("messageId")
                    dialog_partner = resultSet_messages.getString("toAddresses")
                    author = resultSet_messages.getString("fromAddress")
                    date_sent = int(resultSet_messages.getString("dateSentMs"))/1000
                    date_receive = int(resultSet_messages.getString("dateReceivedMs"))/1000
                    text = resultSet_messages.getString("body")
                    subject = resultSet_messages.getString("subject")
                    snippet = resultSet_messages.getString("snippet")
                    attachment = resultSet_messages.getString("joinedAttachmentInfos")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from gmail (" + e.getMessage() + ")")

                status_arr = []
                if subject:
                    status_arr.append("Тема письма: \"".decode('UTF-8'))
                    status_arr.append(subject)
                    status_arr.append("\"; ".decode('UTF-8'))
                if snippet:
                    status_arr.append("Фрагмент письма (снипет): \"".decode('UTF-8'))
                    status_arr.append(snippet)
                    status_arr.append("\"; ".decode('UTF-8'))
                if attachment:
                    status_arr.append("Вложения: \"".decode('UTF-8'))
                    status_arr.append(attachment)
                    status_arr.append("\"; ".decode('UTF-8'))
                status = ' '.join(status_arr)

                art = file.newArtifact(artID_gmail)
                art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, mess_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_SENT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date_sent))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_RCVD.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date_receive))
                art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, author))
                art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, dialog_partner))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, text))
                art.addAttribute(blackboardAttribute(attID_status, imdbIngestModuleFactory.moduleName, status))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        IM_sqlitedb_android.IMDbIngestModule.set_count(self, 1) 
        progressBar.progress(IM_sqlitedb_android.IMDbIngestModule.get_count(self))
        if gmail_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных: Gmail".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Gmail")

        try:
            if 'resultSet_messages' in locals():
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
