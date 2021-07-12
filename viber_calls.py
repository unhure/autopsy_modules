# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os


def viber_calls(self, progressBar, viber_calls_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_viber1 = case.getSleuthkitCase().addArtifactType("TSK_CHATS_VIBER1", "Viber - сообщения".decode('UTF-8'))
    except:
        artID_viber1 = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VIBER1")
    try:
        artID_calllog = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CALLLOG", "Viber - журнал звонков".decode('UTF-8'))
    except:
        artID_calllog = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CALLLOG")

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


    for file in viber_calls_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
            stmt2 = dbConn.createStatement()
        except SQLException as e:
            self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

        try:
            resultSets = stmt.executeQuery("select calls.[_id] as [ID], (select phonebookcontact.[display_name] || ' (ID: ' || phonebookdata.[contact_id] ||')' from phonebookdata, phonebookcontact where calls.[number]=phonebookdata.[data2] and phonebookdata.[contact_id]=phonebookcontact.[native_id]) as [contact], calls.[number] as [number], calls.[viber_call_type] as [type], calls.[date], calls.[duration] from calls order by calls.[date]".decode('UTF-8'))
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for calls or phonebookcontact, phonebookdata table (viber)-1 (" + e.getMessage() + ")")

        try:
            resultSet2 = stmt2.executeQuery("select phonebookcontact.display_name as name, (select phonebookdata.data2 from phonebookdata where phonebookdata.contact_id=phonebookcontact._id) as number from phonebookcontact ORDER BY phonebookcontact.display_name")
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for calls or phonebookcontact, phonebookdata table (viber)-2 (" + e.getMessage() + ")")

        if 'resultSets' in locals():
            while resultSets.next():
                try:
                    date_begin = int(resultSets.getString("date"))/1000
                    # self.log(Level.INFO, "date_begin = " + str(date_begin))
                    date_end = date_begin+resultSets.getInt("duration")
                    contact = resultSets.getString("contact")
                    call_type = resultSets.getInt("type")
                    number = resultSets.getString("number")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")


                art = file.newArtifact(artID_calllog)

                if call_type == 1:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, number))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, "-"))
                elif call_type == 2:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, number))
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, "-"))
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

                if contact is not None:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, contact))
                else:
                    art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(),
                                                         imdbIngestModuleFactory.moduleName, "-"))
        if 'resultSet2' in locals():
            while resultSet2.next():
                try:
                    name = resultSet2.getString("name");
                    number = resultSet2.getString("number");
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")

                # contacts
                try:
                    artID_contacts_viber = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CONTACTS_VIBER", "Viber - контакты".decode('UTF-8'))
                except:
                    artID_contacts_viber = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_VIBER")

                art2 = file.newArtifact(artID_contacts_viber)
                art2.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(),
                                                      imdbIngestModuleFactory.moduleName, name))
                art2.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER.getTypeID(),
                                                      imdbIngestModuleFactory.moduleName, number))
                art2.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TAG_NAME.getTypeID(),
                                                      imdbIngestModuleFactory.moduleName, "Контакты Viber".decode('UTF-8')))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        file_count = IM_sqlitedb_android.IMDbIngestModule.get_count(self) + 1
        IM_sqlitedb_android.IMDbIngestModule.set_count(self, file_count)
        progressBar.progress(file_count)
        if viber_calls_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных:  Viber (звонки)".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Viber (звонки)")
                
        try:
            if 'resultSets' in locals():
                resultSets.close()
            if 'resultSet2' in locals():
                resultSet2.close()
            stmt.close()
            stmt2.close()
            dbConn.close()
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error closing database", ex)
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
