# -*- coding: utf-8 -*-
import jarray
import inspect
import binascii
import os
from java.lang import Class
from java.lang import System
from java.sql  import DriverManager, SQLException
from java.util.logging import Level
from java.io import File
from org.sleuthkit.datamodel import SleuthkitCase
from org.sleuthkit.datamodel import AbstractFile
from org.sleuthkit.datamodel import ReadContentInputStream
from org.sleuthkit.datamodel import BlackboardArtifact
from org.sleuthkit.datamodel import BlackboardAttribute
from org.sleuthkit.autopsy.ingest import IngestModule
from org.sleuthkit.autopsy.ingest.IngestModule import IngestModuleException
from org.sleuthkit.autopsy.ingest import DataSourceIngestModule
from org.sleuthkit.autopsy.ingest import IngestModuleFactoryAdapter
from org.sleuthkit.autopsy.ingest import IngestMessage
from org.sleuthkit.autopsy.ingest import IngestServices
from org.sleuthkit.autopsy.ingest import ModuleDataEvent
from org.sleuthkit.autopsy.coreutils import Logger
from org.sleuthkit.autopsy.casemodule import Case
from org.sleuthkit.autopsy.datamodel import ContentUtils
from org.sleuthkit.autopsy.casemodule.services import Services
from org.sleuthkit.autopsy.casemodule.services import FileManager
# This will work in 4.0.1 and beyond
# from org.sleuthkit.autopsy.casemodule.services import Blackboard
# Factory that defines the name and details of the module and allows Autopsy
# to create instances of the modules that will do the analysis.
class IMDbIngestModuleFactory(IngestModuleFactoryAdapter):

    moduleName = "IM SQliteDB Analyzer"

    def getModuleDisplayName(self):
        return self.moduleName

    def getModuleDescription(self):
        return "Module that extracts info from some IM sqlite databases"

    def getModuleVersionNumber(self):
        return "1.0"

    def isDataSourceIngestModuleFactory(self):
        return True

    def createDataSourceIngestModule(self, ingestOptions):
        return IMDbIngestModule()


# Data Source-level ingest module.  One gets created per data source.
class IMDbIngestModule(DataSourceIngestModule):

    _logger = Logger.getLogger(IMDbIngestModuleFactory.moduleName)

    def log(self, level, msg):
        self._logger.logp(level, self.__class__.__name__, inspect.stack()[1][3], msg)

    def __init__(self):
        self.context = None

    # Where any setup and configuration is done
    # 'context' is an instance of org.sleuthkit.autopsy.ingest.IngestJobContext.
    # See: http://sleuthkit.org/autopsy/docs/api-docs/3.1/classorg_1_1sleuthkit_1_1autopsy_1_1ingest_1_1_ingest_job_context.html
    def startUp(self, context):
        self.context = context
        # Throw an IngestModule.IngestModuleException exception if there was a problem setting up
        # raise IngestModuleException("Oh No!")

    # Where the analysis is done.
    # The 'dataSource' object being passed in is of type org.sleuthkit.datamodel.Content.
    # See: http://www.sleuthkit.org/sleuthkit/docs/jni-docs/4.3/interfaceorg_1_1sleuthkit_1_1datamodel_1_1_content.html
    # 'progressBar' is of type org.sleuthkit.autopsy.ingest.DataSourceIngestModuleProgress
    # See: http://sleuthkit.org/autopsy/docs/api-docs/3.1/classorg_1_1sleuthkit_1_1autopsy_1_1ingest_1_1_data_source_ingest_module_progress.html
    def process(self, dataSource, progressBar):

        # we don't know how much work there is yet
        progressBar.switchToIndeterminate()

        # This will work in 4.0.1 and beyond
        # Use blackboard class to index blackboard artifacts for keyword search
        # blackboard = Case.getCurrentCase().getServices().getBlackboard()

        # Find files named, regardless of parent path
        fileManager = Case.getCurrentCase().getServices().getFileManager()
        vk_files = fileManager.findFiles(dataSource, "vk.db")
        kate_files = fileManager.findFiles(dataSource, "kate.db")
        viber_calls_files = fileManager.findFiles(dataSource, "viber_data")
        viber_messages_files = fileManager.findFiles(dataSource, "viber_messages")
        skype_files=fileManager.findFiles(dataSource, "main.db")
        
        numFiles = len(vk_files)+len(kate_files)+len(viber_calls_files)+len(viber_messages_files)+len(skype_files)
        progressBar.switchToDeterminate(numFiles)
        try:
            artID_vk1 = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_VK1", "ВКонтакте".decode('UTF-8'))
        except:		
            artID_vk1 = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VK1")

        try:
            artID_viber1 = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_VIBER1", "Viber".decode('UTF-8'))
        except:		
            artID_viber1 = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VIBER1")

	#artID_vk1 = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VK1")

        try:
            artID_odnoclassniki = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_OK", "Одноклассники".decode('UTF-8'))
        except:		
            artID_odnoclassniki = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_OK")

            
        try:
            artID_skype = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_SKYPE", "Skype".decode('UTF-8'))
        except:		
            artID_skype = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_SKYPE")

            
        try:
            attID_nr = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_VK_ID", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Идентификатор сообщения".decode('UTF-8'))
        except:		
            self.log(Level.INFO, "Attributes Creation Error, Program Number Runs. ==> ")           
            attID_nr = Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_VK_ID")

        try:
            attID_sender = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_VK_OTPRAV", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, отправивший сообщение".decode('UTF-8'))
        except:		
            self.log(Level.INFO, "Attributes Creation Error, Program Number Runs. ==> ")            
            attID_sender=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_VK_OTPRAV")

        try:
            attID_reciever = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_VK_RECIEVER", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, получивший сообщение".decode('UTF-8'))
        except:		
            self.log(Level.INFO, "Attributes Creation Error, Program Number Runs. ==> ")            
            attID_reciever=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_VK_RECIEVER")

        try:
            attID_status = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_VK_STATUS", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Информация о сообщении".decode('UTF-8'))
        except:		
            self.log(Level.INFO, "Attributes Creation Error, Program Number Runs. ==> ")            
            attID_status=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_VK_STATUS")

           
	for file in vk_files:		
            fileCount = 0;
            # Check if the user pressed cancel while we were busy
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1

            # Save the DB locally in the temp folder. use file id as name to reduce collisions
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))
            
            # Open the DB using JDBC
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK
            
            # Query the contacts table in the database and get all columns. 
            try:
                stmt = dbConn.createStatement()
                resultSet = stmt.executeQuery("select messages.mid as ID, messages.peer as [peer], messages.[time] as [date], (select users.lastname || ' ' || users.firstname from users where users.uid=messages.sender) || ' (id_peer: '|| messages.sender || ')' as [Sender], case messages.sender=messages.peer when 0 then (select users.lastname || ' ' || users.firstname from users where users.uid=messages.peer) || ' (id_peer: ' || messages.peer || ')' else  (select users.lastname || ' ' || users.firstname || ' (id: ' || users.uid || ' - учетная запись мобильного телефона)' from users, messages where messages.peer!=messages.sender and users.uid=messages.sender) end as [User_who_recieved_messages], messages.text as [text], hex(messages.attachments) as attachments from  messages order by messages.[time]".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for contacts table (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK
            
            # Cycle through each row and create artifacts
            while resultSet.next():
                try:		 
                    ID  = resultSet.getString("ID")
                    date = resultSet.getInt("date")
                    sender = resultSet.getString("Sender")
                    if resultSet.getString("User_who_recieved_messages") is None:
                        reciever = "Идентификатор беседы (чата): ".decode('UTF-8')+resultSet.getString("peer")
                    else:
                        reciever = resultSet.getString("User_who_recieved_messages");
		        mess = resultSet.getString("text")
                        tmp_string=binascii.unhexlify(resultSet.getString("attachments"))
                    if tmp_string!="" and tmp_string is not None:
                        status = "Приложение: ".decode('UTF-8')+tmp_string.decode("UTF-8", 'ignore');
                    else:
                        status = " "
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")
                    
                # Make an artifact on the blackboard, TSK_CONTACT and give it attributes for each of the fields
                #art = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_VKdb)
                art = file.newArtifact(artID_vk1)
                
	 	art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MESSAGE_TYPE, 
                                                     IMDbIngestModuleFactory.moduleName, "VK"))
                
                art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, ID))
                
                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, date))
                
                art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, sender))
                art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, reciever))
                

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, mess))

                art.addAttribute(BlackboardAttribute(attID_status, IMDbIngestModuleFactory.moduleName, status))
                
            # Fire an event to notify the UI and others that there are new artifacts
            IngestServices.getInstance().fireModuleDataEvent(
                ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
            
            # Clean up
            stmt.close()
            dbConn.close()
            os.remove(lclDbPath)
            
            # After all databases, post a message to the ingest messages in box.
            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA, "IM SQliteDB Analyzer", "Found %d vk_db files" % fileCount)
            IngestServices.getInstance().postMessage(message)

	for file in kate_files:		
	    fileCount = 0
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1

            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))
            
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK
            
            try:
                stmt = dbConn.createStatement()
                resultSet = stmt.executeQuery("select message_id, (select users.last_name || ' ' || users.first_name || ' (' || messages.[account_id] || ')' from users where messages.[account_id]=users._id) as [sender_name], (select users.last_name || ' ' || users.first_name || ' (' || users._id || ')' from users where messages.uid=users._id) as [reciever_name], (select users.nickname from users where messages.uid=users._id) as [nickname], (select users.birthdate from users where messages.uid=users._id) as [rec_birthday], messages.title as [name_mess], messages.body as [text], messages.date as [date], case messages.read_state when 1 then 'Прочитано' else 'Не прочитано' end as [status1], case messages.is_out when 0 then 'Входящее' else 'Исходящее' end as [status2] from messages order by messages.date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for contacts table (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK

            while resultSet.next():
                try:
		    mess_id  = resultSet.getString("message_id")
		    date = resultSet.getInt("date")
		    sender = resultSet.getString("sender_name")
		    mess = resultSet.getString("text")
		    info_arr = []
                    info_arr.append(resultSet.getString("reciever_name"))
		    if  resultSet.getString("nickname")!="" and resultSet.getString("nickname") is not None:
                        info_arr.append(" (логин: ".decode('UTF-8')) 
                        info_arr.append(resultSet.getString("nickname"))
                        info_arr.append(") ".decode('UTF-8'))
		    if resultSet.getString("rec_birthday")!="" and resultSet.getString("rec_birthday") is not None:
                        info_arr.append(", День рождения: ".decode('UTF-8'))
                        info_arr.append(resultSet.getString("rec_birthday"));
                    reciever=''.join(info_arr);                       
                    status_arr=[]
                    if  resultSet.getString("name_mess")!="" and resultSet.getString("name_mess") is not None:
                        status_arr.append("Название переписки: \"".decode('UTF-8'))
                        status_arr.append(resultSet.getString("name_mess"))
                        status_arr.append("\"; ".decode('UTF-8'))
                        status_arr.append("Статус сообщения: ".decode('UTF-8'))
                        status_arr.append(resultSet.getString("status1"))
                        status_arr.append(", ".decode('UTF-8'))
                        status_arr.append(resultSet.getString("status2"))
                    status=''.join(status_arr)
                        #"Название сообщения:"+resultSet.getString("name_mess")+"Статус сообщения: "+resultSet.getString("status")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")
                    
                # Make an artifact on the blackboard, TSK_CONTACT and give it attributes for each of the fields
                #art = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_VKdb)
                art = file.newArtifact(artID_vk1)
                
	 	art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MESSAGE_TYPE, 
                                                     IMDbIngestModuleFactory.moduleName, "Kate Mobile"))
                
                art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, mess_id))
                
                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, date))

                if resultSet.getString("status2")=="Входящее":
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, reciever))
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, sender))
                else:
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, sender))
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, reciever))
                

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, mess))
                art.addAttribute(BlackboardAttribute(attID_status, IMDbIngestModuleFactory.moduleName, status)) 
                
            IngestServices.getInstance().fireModuleDataEvent(
                ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
            
            # Clean up
            stmt.close()
            dbConn.close()
            os.remove(lclDbPath)

            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                                  "IM SQliteDB Analyzer", "Found %d kate_mobile_db files" % fileCount)
            IngestServices.getInstance().postMessage(message)

        #Extract viber calls
	for file in viber_calls_files:		
	    fileCount = 0
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK
            
            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1

            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))
            
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK
            
            try:
                stmt = dbConn.createStatement()
                resultSets = stmt.executeQuery("select calls.[_id] as [ID], (select phonebookcontact.[display_name] || ' (ID: ' || phonebookdata.[contact_id] ||')' from phonebookdata, phonebookcontact where calls.[number]=phonebookdata.[data2] and phonebookdata.[contact_id]=phonebookcontact.[native_id]) as [contact], calls.[number] as [number], calls.[viber_call_type] as [type], calls.[date], calls.[duration] from calls order by calls.[date]".decode('UTF-8'))               
                resultSet2 = dbConn.createStatement().executeQuery("select phonebookcontact.display_name as name, phonebookdata.data2 as number from phonebookcontact, phonebookdata where phonebookcontact._id=phonebookdata.contact_id ORDER BY phonebookcontact.display_name".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for calls or phonebookcontact, phonebookdata table (viber) (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK

            while resultSets.next():
                try:		 
                    date_begin = int(resultSets.getString("date"))/1000;                   
                    #self.log(Level.INFO, "date_begin = " + str(date_begin))
                    date_end = date_begin+resultSets.getInt("duration");
                    contact = resultSets.getString("contact");
                    call_type = resultSets.getInt("type");
                    number = resultSets.getString("number");
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")
                    
                art = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_CALLLOG)

                if call_type == 1:
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, number))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "-"));
                elif call_type == 2:
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, number));
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "-"));
                    
                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_START.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, date_begin))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_END.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, date_end))

                if call_type == 1:
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DIRECTION.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "Входящий".decode('UTF-8')));
                elif call_type == 2:
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DIRECTION.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "Исходящий".decode('UTF-8')));

                if contact is not None:
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, contact));
                else:
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "-"))

            while resultSet2.next():
                try:		 
                    name = resultSet2.getString("name");
                    number = resultSet2.getString("number");
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")
                    
                art2 = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_CONTACT)
                
                art2.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                      IMDbIngestModuleFactory.moduleName, name))

                art2.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER.getTypeID(), 
                                                      IMDbIngestModuleFactory.moduleName, number))
                
                art2.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TAG_NAME.getTypeID(), 
                                                      IMDbIngestModuleFactory.moduleName, "Контакты Viber".decode('UTF-8')))
                
            IngestServices.getInstance().fireModuleDataEvent(
                ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
            stmt.close()
            dbConn.close()
            os.remove(lclDbPath)

            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                                  "IM SQliteDB Analyzer", "Found %d viber_calls files" % fileCount)
            IngestServices.getInstance().postMessage(message)

            #Extract viber messages
	for file in viber_messages_files:		
	    fileCount = 0
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK
            
            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1

            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))
            
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK
            
            try:
                stmt = dbConn.createStatement()
                resultSet = stmt.executeQuery("select (select case when display_name IS NULL then contact_id else display_name || ' ' || number end from participants_info, participants where messages.participant_id=participants._id and participants.participant_info_id=participants_info._id) || ' (' || messages.address || ')' as name, messages.type as type, messages.body as text, messages.date as date from messages ORDER BY messages.date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for contacts table (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK

            while resultSet.next():
                try:                
                    contact = resultSet.getString("name");
                    mess_type = resultSet.getInt("type");
                    date = int(resultSet.getString("date"))/1000;
                    text = resultSet.getString("text")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")
                    
                art = file.newArtifact(artID_viber1)                

                if mess_type == 0:
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, contact))
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, " "))
                elif mess_type == 1:
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, contact))
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, " "))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, text))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, date))
                
            IngestServices.getInstance().fireModuleDataEvent(
                ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
            stmt.close()
            dbConn.close()
            os.remove(lclDbPath)

            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                                  "IM SQliteDB Analyzer", "Found %d viber_messages files" % fileCount)
            IngestServices.getInstance().postMessage(message)

            #Extract skype messages
	for file in skype_files:		
	    fileCount = 0
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK
            
            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1

            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))
            
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK
            
            try:
                stmt = dbConn.createStatement()
                resultSet_messages = stmt.executeQuery("select Messages.[id], Messages.[timestamp], Messages.[dialog_partner], Messages.[author], Messages.[body_xml], Messages.[reason], Messages.[convo_id], Messages.[type] from Messages where Messages.[chatmsg_type]=3 order by Messages.[timestamp]".decode('UTF-8'))
                account_info = dbConn.createStatement().executeQuery("select Accounts.[fullname], Accounts.[skypename], Accounts.[emails], Accounts.[phone_home], Accounts.[phone_mobile] from Accounts".decode('UTF-8'))
                resultSet_contacts = dbConn.createStatement().executeQuery("select Contacts.[fullname], Contacts.[skypename],  Contacts.[birthday], Contacts.[phone_home], Contacts.[phone_office],  Contacts.[phone_mobile], Contacts.[emails], Contacts.[homepage], Contacts.[about] from Contacts".decode('UTF-8'))
                resultSet_calls = dbConn.createStatement().executeQuery("select Calls.[id], CallMembers.[type], Calls.[host_identity], CallMembers.[real_identity],  CallMembers.[dispname] , Calls.[begin_timestamp], Calls.[duration],  CallMembers.[guid] from Calls, CallMembers where Calls.[id]=CallMembers.[call_db_id]".decode('UTF-8'))                
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for skype (" + e.getMessage() + ")")
                return IngestModule.ProcessResult.OK

            account_tmp=[];            
            try:
                if account_info.getString("fullname") is not None:
                    account_tmp.append(account_info.getString("fullname"));
                    account_tmp.append(" ".decode('UTF-8'));
                    account_tmp.append(account_info.getString("skypename"));
                    account_tmp.append(" (".decode('UTF-8'));
                    account_tmp.append(account_info.getString("emails"));
                    account_tmp.append(")".decode('UTF-8'));
                    account = ''.join(account_tmp);
                else:
                    account_tmp.append(account_info.getString("skypename"));
                    account_tmp.append(" (".decode('UTF-8'));
                    account_tmp.append(account_info.getString("emails"));
                    account_tmp.append(")".decode('UTF-8'));
                    account = ''.join(account_tmp);
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for account table (skype) (" + e.getMessage() + ")")
                
            
            while resultSet_messages.next():
                try:
                    mess_id = resultSet_messages.getString("id");
                    dialog_partner = resultSet_messages.getString("dialog_partner");
                    author = resultSet_messages.getString("author");
                    date = int(resultSet_messages.getString("timestamp"));
                    text = resultSet_messages.getString("body_xml");
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from messages table (skype) (" + e.getMessage() + ")")
                    
                    
                art = file.newArtifact(artID_skype)                

                art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, mess_id))
                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, date))
                
                if dialog_partner is None:
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, account))
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, author))
                else:
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, dialog_partner))
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, account))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, text))


            while resultSet_contacts.next():
                try:
                    if resultSet_contacts.getString("fullname") is None:
                        fullname = resultSet_contacts.getString("skypename");
                    else:
                        tmp=[]
                        tmp.append(resultSet_contacts.getString("fullname"));
                        tmp.append(" ".decode('UTF-8'));
                        tmp.append(resultSet_contacts.getString("skypename"));
                        fullname = "".join(tmp)
                        
                    home_phone = resultSet_contacts.getString("phone_home");
                    phone_office = resultSet_contacts.getString("phone_office")
                    phone_mobile = resultSet_contacts.getString("phone_mobile")
                    email = resultSet_contacts.getString("emails")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from contacts table (skype) (" + e.getMessage() + ")")

                art = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_CONTACT)                

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, fullname))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_HOME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, home_phone))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_OFFICE.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, phone_office))

                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_MOBILE.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, phone_mobile))
                
                art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TAG_NAME.getTypeID(), 
                                                     IMDbIngestModuleFactory.moduleName, "Контакты Skype".decode('UTF-8')))


                while resultSet_calls.next():
                    try:		 
                        date_begin = int(resultSets_calls.getString("begin_timestamp"))/1000;                   
                        #self.log(Level.INFO, "date_begin = " + str(date_begin))
                        date_end = date_begin+resultSets_calls.getInt("duration");
                        contact = resultSets_calls.getString("real_identity");
                        dispname = resultSets_calls.getString("dispname");
                        call_type = resultSets_calls.getInt("type");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from calls table (skype) (" + e.getMessage() + ")")
                        
                    art = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_CALLLOG)
                    
                    tmp=[]
                    tmp.append(contact)
                    tmp.append(" ".decode('UTF-8'));
                    tmp.append(dispname)
                    fullname = "".join(tmp)
                    
                    if call_type == 1:
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, fullname))
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, "-"));
                    elif call_type == 2:
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, fullname));
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, "-"));
                        
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_START.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, date_begin))
                        
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_END.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, date_end))
                        
                        if call_type == 1:
                            art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DIRECTION.getTypeID(), 
                                                                 IMDbIngestModuleFactory.moduleName, "Входящий".decode('UTF-8')));
                        elif call_type == 2:
                            art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DIRECTION.getTypeID(), 
                                                                 IMDbIngestModuleFactory.moduleName, "Исходящий".decode('UTF-8')));
                            
                            art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                                 IMDbIngestModuleFactory.moduleName, fullname));

                    
                IngestServices.getInstance().fireModuleDataEvent(
                    ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                    BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
                stmt.close()
                dbConn.close()
                os.remove(lclDbPath)

        message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                              "IM SQliteDB Analyzer", "Found %d skype files" % fileCount)
        IngestServices.getInstance().postMessage(message)

            

        return IngestModule.ProcessResult.OK
