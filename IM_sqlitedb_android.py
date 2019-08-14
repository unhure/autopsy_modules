# -*- coding: utf-8 -*-
from java.io import File
from java.io import IOException
from java.lang import Class
from java.lang import ClassNotFoundException
from java.lang import System
from java.lang import String
from java.sql import Connection
from java.sql import DriverManager
from java.sql import SQLException
from java.sql import ResultSet
from java.sql import Statement
from java.util.logging import Level
from java.util import ArrayList
from org.sleuthkit.datamodel import SleuthkitCase
from org.sleuthkit.datamodel import AbstractFile
from org.sleuthkit.datamodel import Content
from org.sleuthkit.datamodel import Account
from org.sleuthkit.datamodel import Relationship
from org.sleuthkit.datamodel import TskCoreException
from org.sleuthkit.datamodel import ReadContentInputStream
from org.sleuthkit.datamodel import BlackboardArtifact
from org.sleuthkit.datamodel import BlackboardAttribute
from org.sleuthkit.datamodel.BlackboardAttribute import ATTRIBUTE_TYPE
from org.sleuthkit.autopsy.datamodel import ContentUtils
from org.sleuthkit.autopsy.ingest import IngestModule
from org.sleuthkit.autopsy.ingest.IngestModule import IngestModuleException
from org.sleuthkit.autopsy.ingest import DataSourceIngestModule
from org.sleuthkit.autopsy.ingest import IngestModuleFactoryAdapter
from org.sleuthkit.autopsy.ingest import IngestMessage
from org.sleuthkit.autopsy.ingest import IngestJobContext
from org.sleuthkit.autopsy.ingest import IngestServices
from org.sleuthkit.autopsy.ingest import ModuleDataEvent
from org.sleuthkit.autopsy.coreutils import Logger
from org.sleuthkit.autopsy.coreutils import MessageNotifyUtil
from org.sleuthkit.autopsy.casemodule import Case
from org.sleuthkit.autopsy.casemodule.services import Services
from org.sleuthkit.autopsy.casemodule.services import FileManager
from org.sleuthkit.autopsy.casemodule.services import Blackboard
# This will work in 4.0.1 and beyond
# from org.sleuthkit.autopsy.casemodule.services import Blackboard
# Factory that defines the name and details of the module and allows Autopsy
# to create instances of the modules that will do the analysis.

import jarray
import inspect
import binascii
import os
import traceback
import general
from textwrap import wrap

class IMDbIngestModuleFactory(IngestModuleFactoryAdapter):

    moduleName = "IM SQLiteDB Analyzer"

    def getModuleDisplayName(self):
        return self.moduleName

    def getModuleDescription(self):
        return "Модуль, копирующий информацию из некоторых баз данных sqlite".decode('UTF-8')

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
        skype_files=fileManager.findFiles(dataSource, "%.db", "com.skype.raider")
        gmail_files=fileManager.findFiles(dataSource, "mailstore.%.db", "com.google.android.%")
        aquamail_files=fileManager.findFiles(dataSource, "Messages.%", "org.kman.AquaMail")
        odnoklassniki_files=fileManager.findFiles(dataSource, "odnklassniki.db")
        odnoklassniki_files_tam=fileManager.findFiles(dataSource, "tamtam_messages")

        numFiles = len(vk_files)+len(kate_files)+len(viber_calls_files)+len(viber_messages_files)+len(skype_files)+len(gmail_files)+len(odnoklassniki_files)+len(odnoklassniki_files_tam)
        self.log(Level.INFO, "Found " + str(numFiles) + " files")
        progressBar.switchToDeterminate(numFiles)
        social_app=[]
        fileCount = 0

# create new artifact type
        try:
            artID_vk1 = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_VK1", "ВКонтакте - сообщения".decode('UTF-8'))
        except:		
            artID_vk1 = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VK1")

        try:
            artID_contact = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CONTACTS_VK", "ВКонтакте - контакты".decode('UTF-8'))
        except:		
            artID_contact = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_VK")

        try:
            artID_vk_kate = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_VK2", "Kate Mobile (ВКонтакте) - сообщения".decode('UTF-8'))
        except:		
            artID_vk_kate = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VK2")

        try:
            artID_contact_kate = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CONTACTS_KATE", "Kate Mobile (ВКонтакте) - контакты".decode('UTF-8'))
        except:		
            artID_contact_kate = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_KATE")

        try:
            artID_wall = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_WALL", "Kate Mobile (ВКонтакте) - стена".decode('UTF-8'))
        except:		
            artID_wall = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_WALL")

        try:
            artID_viber1 = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_VIBER1", "Viber - сообщения".decode('UTF-8'))
        except:		
            artID_viber1 = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VIBER1")

        try:
            artID_contacts_skype = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CONTACTS_SKYPE", "Skype - контакты".decode('UTF-8'))
        except:		    
            artID_contacts_skype = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_SKYPE")

        try:
            artID_calllog_skype = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CALLLOG_SKYPE", "Skype - журнал звонков".decode('UTF-8'))
        except:		
            artID_calllog_skype = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CALLLOG_SKYPE")

        try:
            artID_skype = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_SKYPE", "Skype - сообщения".decode('UTF-8'))
        except:		
            artID_skype = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_SKYPE")

        try:
            artID_odnoclassniki = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_OK", "Одноклассники - сообщения".decode('UTF-8'))
        except:		
            artID_odnoclassniki = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_OK")

        try:
            artID_contacts_odnoclassniki = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CONTACTS_ODNOCLASSNIKI", "Одноклассники - контакты".decode('UTF-8'))
        except:		    
            artID_contacts_odnoclassniki = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_ODNOCLASSNIKI")

        try:
            artID_gmail = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_GMAIL", "Gmail - электронная почта".decode('UTF-8'))
        except:		
            artID_gmail = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_GMAIL")       

        try:
            artID_aquamail = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_AQUAMAIL", "Aquamail - электронная почта".decode('UTF-8'))
        except:		
            artID_aquamail = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_AQUAMAIL")

#create new attributes to artifact                
        try:
            attID_nr = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_MESS_ID", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Идентификатор сообщения".decode('UTF-8'))
        except:		
            attID_nr = Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_MESS_ID")

        try:
            attID_sender = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_MESS_OTPRAV", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, отправивший сообщение".decode('UTF-8'))
        except:		
            attID_sender=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_MESS_OTPRAV")

        try:
            attID_reciever = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_MESS_RECIEVER", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Пользователь, получивший сообщение".decode('UTF-8'))
        except:		
            attID_reciever=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_MESS_RECIEVER")

        try:
            attID_status = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_MESS_STATUS", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Дополнительная информация".decode('UTF-8'))
        except:		
            attID_status=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_MESS_STATUS")

        try:
            attID_companion = Case.getCurrentCase().getSleuthkitCase().addArtifactAttributeType("TSK_MESS_COMPANION", BlackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING, "Собеседник".decode('UTF-8'))
        except:		
            attID_companion=Case.getCurrentCase().getSleuthkitCase().getAttributeType("TSK_MESS_COMPANION")    

#extract vk             
        for file in vk_files:		
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
                stmt = dbConn.createStatement()
                stmt2 = dbConn.createStatement()
                stmt3 = dbConn.createStatement()
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

            # Query the contacts table in the database and get all columns. 
            try:
                resultSet = stmt.executeQuery("select messages.mid as ID, messages.peer as [peer], messages.[time] as [date], (select users.lastname || ' ' || users.firstname from users where users.uid=messages.sender) || ' (id_peer: '|| messages.sender || ')' as [Sender], case messages.sender=messages.peer when 0 then (select users.lastname || ' ' || users.firstname from users where users.uid=messages.peer) || ' (id_peer: ' || messages.peer || ')' else  (select users.lastname || ' ' || users.firstname || ' (id: ' || users.uid || ' - учетная запись мобильного телефона)' from users, messages where messages.peer!=messages.sender and users.uid=messages.sender) end as [User_who_recieved_messages], messages.text as [text], hex(messages.attachments) as attachments from  messages order by messages.[time]".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for vk messages (" + e.getMessage() + ")")

            try:
                resultSet_contacts = stmt2.executeQuery("select users.uid as [uid], users.lastname as [lastname], users.firstname as [firstname], users.photo_small as [photo], users.[is_friend] as [friend], birthdays.bday || '.' || birthdays.bmonth || '.' || birthdays.byear as [birthdays] from users left join birthdays on birthdays.uid=users.uid ORDER BY users.lastname")
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for vk contacts 1 (" + e.getMessage() + ")")

            try:
                resultSet_contacts = stmt3.executeQuery("select users.uid as [uid], users.lastname as [lastname], users.firstname as [firstname], users.photo_small as [photo], users.[is_friend] as [friend], users.bday || '.' || users.bmonth || '.' || users.byear as [birthdays] from users ORDER BY users.lastname")
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for vk contacts 2 (" + e.getMessage() + ")")

            # Cycle through each row and create artifacts
            if 'resultSet' in locals():
                while resultSet.next():
                    try:		 
                        ID  = resultSet.getString("ID")
                        date = resultSet.getInt("date")
                        sender = resultSet.getString("Sender")
                        if resultSet.getString("User_who_recieved_messages") is None:
                            reciever = "Идентификатор беседы (чата): ".decode('UTF-8')+resultSet.getString("peer")
                            mess = resultSet.getString("text")
                            tmp_string=binascii.unhexlify(resultSet.getString("attachments"))
                        else:
                            reciever = resultSet.getString("User_who_recieved_messages");
                            mess = resultSet.getString("text")
                            tmp_string=binascii.unhexlify(resultSet.getString("attachments"))
                        if tmp_string!="" and tmp_string is not None:
                            status = "Приложение: ".decode('UTF-8')+tmp_string.decode("UTF-8", 'ignore');
                        else:
                            status = " "
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from vk message table (" + e.getMessage() + ")")

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

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
            if 'resultSet_contacts' in locals():
                while resultSet_contacts.next():
                    try:
                        user = resultSet_contacts.getString("firstname")+" "+resultSet_contacts.getString("lastname")+" ("+resultSet_contacts.getString("uid")+")"
                        photo = resultSet_contacts.getString("photo")
                        friend = resultSet_contacts.getInt("friend")
                        birthdays = resultSet_contacts.getString("birthdays")
                        if friend == 1 and  birthdays is not None:
                            status = "Входит в список друзей. Указанный пользователем день рождения: ".decode('UTF-8')+birthdays
                        elif birthdays is None and friend == 1:
                            status = "Входит в список друзей.".decode('UTF-8')
                        elif birthdays is not None and friend == 0:
                            status = "Указанный пользователем день рождения: ".decode('UTF-8')+birthdays
                        else:
                            status = " "
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from vk contacts table (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_contact)

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, user))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, photo))

                    art.addAttribute(BlackboardAttribute(attID_status, IMDbIngestModuleFactory.moduleName, status))

                    # Fire an event to notify the UI and others that there are new artifacts
                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            try:
                if 'resultSet' in locals():
                    resultSet.close()                    
                if 'resultSet_contacts' in locals():
                    resultSet_contacts.close()
                stmt.close()                    
                stmt2.close()
                stmt3.close()
                dbConn.close()
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error closing database", ex)
                self._logger.log(Level.SEVERE, traceback.format_exc())

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
                
            progressBar.progress(fileCount)
            if vk_files.index(file) == 0:
                social_app.append("VK (ВКонтакте)".decode('UTF-8'))

        for file in kate_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1

            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
                stmt = dbConn.createStatement()
                stmt2 = dbConn.createStatement()
                stmt3 = dbConn.createStatement()
                stmt4 = dbConn.createStatement()
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")
            
            try:                
                resultSet = stmt.executeQuery("select message_id, (select users.last_name || ' ' || users.first_name || ' (' || messages.[account_id] || ')' from users where messages.[account_id]=users._id) as [sender_name], (select users.last_name || ' ' || users.first_name || ' (' || users._id || ')' from users where messages.uid=users._id) as [reciever_name], (select users.nickname from users where messages.uid=users._id) as [nickname], (select users.birthdate from users where messages.uid=users._id) as [rec_birthday], messages.title as [name_mess], messages.body as [text], messages.date as [date], case messages.read_state when 1 then 'Прочитано' else 'Не прочитано' end as [status1], messages.is_out as [status2] from messages order by messages.date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for kate table (" + e.getMessage() + ") resultSet")

            try:
                resultSet_contacts = stmt2.executeQuery("select users.[last_name] || ' ' || users.[first_name] || '  (' || users.[_id] ||  ')' as [id], users.[nickname], users.[photo_medium_rec] as [photo], users.[birthdate], users.[mobile_phone], users.[home_phone], users.[status] from  users order by users.[_id]")
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for kate table (" + e.getMessage() + ") resultSet_contacts")

            try:
                resultSet_wall = stmt3.executeQuery("select _id, case when from_id > 0 then (select first_name || ' ' || last_name from users where users._id=wall.from_id) else 'Сообщение группы: ' || (select groups.name from groups where groups._id=wall.from_id*-1) end as [user], date, case when text='' then (select attachments.type || ': ' || case attachments.type when 'photo' then  (select photos.src_big from photos where attachments.[photo_id]=photos.[photo_id]) when 'video' then  (select video.[title] || ' ' || video.[image_big] from video where attachments.[video_id]=video.[video_id]) when 'link' then attachments.link_url when 'audio' then  (select audio.[artist]  || ' ' || audio.[title] from audio where attachments.[audio_id]=audio.[audio_id]) when 'poll' then  (select poll.[question] from poll where attachments.[poll_id]=poll.[poll_id]) when 'page' then attachments.[page_title] || ' (page id:' || attachments.[page_id] || ')' when 'geo' then attachments.[geo_lat] || ', ' || attachments.[geo_lon] when 'doc' then  (select docs.title  || ' ' || docs.[ext] from attachments, docs where attachments.[doc_id]=docs.[doc_id]) else ' ' end from attachments where attachments.[content_id]=wall._id) else text end as [text] from wall order by date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for kate table (" + e.getMessage() + ") resultSet_wall")               

            try:
                resultSet_wall = stmt4.executeQuery("select _id, case when from_id > 0 then (select first_name || ' ' || last_name from users where users._id=wall.from_id) else 'Сообщение группы: ' || (select groups.name from groups where groups._id=wall.from_id*-1) end as [user], date, case when text='' then (select attachments.type || ': ' || case attachments.type when 'photo' then  (select photos.src_big from photos where attachments.[photo_id]=photos.[_id]) when 'video' then  (select video.[title] || ' ' || video.[image_big] from video where attachments.[video_id]=video.[video_id]) when 'link' then attachments.link_url when 'audio' then  (select audio.[artist]  || ' ' || audio.[title] from audio where attachments.[audio_id]=audio.[audio_id]) when 'poll' then  (select poll.[question] from poll where attachments.[poll_id]=poll.[poll_id]) when 'page' then attachments.[page_title] || ' (page id:' || attachments.[page_id] || ')' when 'geo' then attachments.[geo_lat] || ', ' || attachments.[geo_lon] when 'doc' then  (select docs.title  || ' ' || docs.[ext] from attachments, docs where attachments.[doc_id]=docs.[_id]) else ' ' end from attachments where attachments.[content_id]=wall._id) else text end as [text] from wall order by date".decode('UTF-8'))
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for kate table (" + e.getMessage() + ") resultSet_wall_2")

            if 'resultSet' in locals():
                while resultSet.next():
                    try:
                        mess_id  = resultSet.getString("message_id")
                        date = resultSet.getInt("date")
                        sender = resultSet.getString("sender_name")
                        mess = resultSet.getString("text")
                        nickname = resultSet.getString("nickname")
                        birthday = resultSet.getString("rec_birthday")
                        name_mess = resultSet.getString("name_mess")
                        info_arr = []
                        info_arr.append(resultSet.getString("reciever_name"))
                        if  nickname!="" and nickname is not None:
                            info_arr.append(" (логин: ".decode('UTF-8')) 
                            info_arr.append(nickname)
                            info_arr.append(") ".decode('UTF-8'))
                        if birthday!="" and birthday is not None:
                            info_arr.append(", День рождения: ".decode('UTF-8'))
                            info_arr.append(birthday);
                        reciever=''.join(info_arr);                       
                        status_arr=[]
                        if  name_mess!="" and name_mess is not None:
                            status_arr.append("Название переписки: \"".decode('UTF-8'))
                            status_arr.append(name_mess)
                            status_arr.append("\"; ".decode('UTF-8'))
                            status_arr.append("Статус сообщения: ".decode('UTF-8'))
                            status_arr.append(resultSet.getString("status1"))
                        status=''.join(status_arr)
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from kate message table (" + e.getMessage() + ")")
                        
                    art = file.newArtifact(artID_vk_kate)
                    
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MESSAGE_TYPE, 
                                                         IMDbIngestModuleFactory.moduleName, "Kate Mobile"))
                    
                    art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, mess_id))
                    
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date))
                    
                    if resultSet.getInt("status2")==0:
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

            if 'resultSet_contacts' in locals():
                while resultSet_contacts.next():
                    try:
                        name  = resultSet_contacts.getString("id")
                        photo_link = resultSet_contacts.getString("photo")
                        mobile_phone = resultSet_contacts.getString("mobile_phone")
                        home_phone = resultSet_contacts.getString("home_phone")
                        nickname = resultSet_contacts.getString("nickname")
                        birthdate = resultSet_contacts.getString("birthdate")
                        status = resultSet_contacts.getString("status")
                        status_arr=[]
                        if  nickname is not None and nickname!="":
                            status_arr.append("Псевдоним пользователя: \"".decode('UTF-8'))
                            status_arr.append(nickname)
                            status_arr.append("\"; ".decode('UTF-8'))
                        if birthdate is not None and birthdate!="":
                            status_arr.append("Указанный пользователем день рождения: ".decode('UTF-8'))
                            status_arr.append(birthdate)
                            status_arr.append("; ".decode('UTF-8'))
                        if status is not None and status!="":    
                            status_arr.append("Указанный пользователем статус: ".decode('UTF-8'))
                            status_arr.append(status)
                            status=''.join(status_arr)
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from kate contacts table (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_contact_kate)

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, name))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, photo_link))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_MOBILE.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, mobile_phone))              

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_HOME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, home_phone))

                    art.addAttribute(BlackboardAttribute(attID_status, IMDbIngestModuleFactory.moduleName, status))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            #wall
            if 'resultSet_wall' in locals():                    
                while resultSet_wall.next():
                    try:
                        post_id  = resultSet_wall.getString("_id")
                        user = resultSet_wall.getString("user")
                        date = resultSet_wall.getInt("date")
                        text = resultSet_wall.getString("text")
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from kate wall table (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_wall)

                    art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, post_id))
                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, user))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, text))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            try:
                if 'resultSet' in locals():
                    resultSet.close()
                if 'resultSet_contacts' in locals():
                    resultSet_contacts.close()
                if 'resultSet_wall' in locals():
                    resultSet_wall.close()
                stmt.close()
                stmt2.close()
                stmt3.close()
                stmt4.close()
                dbConn.close()
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error closing database", ex)
                self._logger.log(Level.SEVERE, traceback.format_exc())

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if kate_files.index(file) == 0:
                social_app.append("Kate Mobile (ВКонтакте)".decode('UTF-8'))
#End vk

#Extract viber
        for file in viber_calls_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
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
                        date_begin = int(resultSets.getString("date"))/1000;                   
                        #self.log(Level.INFO, "date_begin = " + str(date_begin))
                        date_end = date_begin+resultSets.getInt("duration");
                        contact = resultSets.getString("contact");
                        call_type = resultSets.getInt("type");
                        number = resultSets.getString("number");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")

                    #calllog
                    try:
                        artID_calllog = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CALLLOG", "Viber - журнал звонков".decode('UTF-8'))
                    except:		
                        artID_calllog = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CALLLOG")    

                    art = file.newArtifact(artID_calllog)

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
            if 'resultSet2' in locals():
                while resultSet2.next():
                    try:		 
                        name = resultSet2.getString("name");
                        number = resultSet2.getString("number");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from contacts table (" + e.getMessage() + ")")

                    #contacts
                    try:
                        artID_contacts_viber = Case.getCurrentCase().getSleuthkitCase().addArtifactType( "TSK_CHATS_CONTACTS_VIBER", "Viber - контакты".decode('UTF-8'))
                    except:		    
                        artID_contacts_viber = Case.getCurrentCase().getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_VIBER")

                    art2 = file.newArtifact(artID_contacts_viber)

                    art2.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                          IMDbIngestModuleFactory.moduleName, name))

                    art2.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER.getTypeID(), 
                                                          IMDbIngestModuleFactory.moduleName, number))

                    art2.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TAG_NAME.getTypeID(), 
                                                          IMDbIngestModuleFactory.moduleName, "Контакты Viber".decode('UTF-8')))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

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
                self._logger.log(Level.SEVERE, traceback.format_exc())

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if viber_calls_files.index(file) == 0:
                social_app.append("Viber (звонки)".decode('UTF-8'))

        #Extract viber messages
        for file in viber_messages_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
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

            if  base_version == 0:
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
                        contact = resultSet.getString("name");
                        contact2 = resultSet.getString("name2");
                        account = resultSet.getString("account");
                        mess_type = resultSet.getInt("type");
                        date = int(resultSet.getString("date"))/1000;
                        text = resultSet.getString("text")
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from viber messages table (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_viber1)                

                    if mess_type == 0:
                        art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, contact))
                        art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, account))
                    elif mess_type == 1:
                        art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, contact2))
                        art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, contact))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, text))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            try:
                if 'resultSet' in locals():
                    resultSet.close()
                stmt.close()
                stmt1.close()
                dbConn.close()
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error closing database", ex)
                self._logger.log(Level.SEVERE, traceback.format_exc())    

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if viber_messages_files.index(file) == 0:
                social_app.append("Viber (сообщения)".decode('UTF-8'))
#End viber

#Extract skype
        for file in skype_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
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

            account_tmp=[];
            if 'account_info' in locals():
                try:
                    if account_info.getString("fullname") is not None:
                        account_tmp.append(account_info.getString("fullname"));
                        account_tmp.append(" Логин: ".decode('UTF-8'));
                        account_tmp.append(account_info.getString("skypename"));
                        user_email=account_info.getString("emails")
                        if user_email!="-":
                            account_tmp.append(" (".decode('UTF-8'));
                            account_tmp.append(user_email);
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

            if 'resultSet_messages' in locals():
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

            if 'resultSet_contacts' in locals():
                while resultSet_contacts.next():
                    try:
                        skypename=resultSet_contacts.getString("skypename")
                        fullname=resultSet_contacts.getString("fullname")
                        if fullname is None:
                            fullname = resultSet_contacts.getString("skypename");
                        else:
                            tmp=[]
                            tmp.append(resultSet_contacts.getString("fullname"));
                            if skypename is not None:
                                tmp.append(" (Логин: ".decode('UTF-8'))
                                tmp.append(resultSet_contacts.getString("skypename"));
                                tmp.append(")".decode('UTF-8'));
                            fullname = "".join(tmp)

                        home_phone = resultSet_contacts.getString("phone_home");
                        phone_office = resultSet_contacts.getString("phone_office")
                        phone_mobile = resultSet_contacts.getString("phone_mobile")
                        email = resultSet_contacts.getString("emails")
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from contacts table (skype) (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_contacts_skype)                

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, fullname))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_HOME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, home_phone))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_OFFICE.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, phone_office))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_MOBILE.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, phone_mobile))

            if 'resultSet_calls' in locals():
                while resultSet_calls.next():
                    try:		 
                        date_begin = int(resultSet_calls.getString("begin_timestamp"));                   
                        #self.log(Level.INFO, "date_begin = " + str(date_begin))
                        date_end = date_begin+resultSet_calls.getInt("duration");
                        contact = resultSet_calls.getString("identity");
                        dispname = resultSet_calls.getString("dispname");
                        call_type = resultSet_calls.getInt("type");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from calls table (skype) (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_calllog_skype)

                    tmp=[]
                    if dispname is not None:
                        tmp.append(dispname)
                    if contact is not None:
                        tmp.append(" (".decode('UTF-8'))
                        tmp.append(contact)
                        tmp.append(")".decode('UTF-8'));
                    fullname = "".join(tmp)

                    if call_type == 1:
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, fullname))
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, account));
                    elif call_type == 2:
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_TO.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, fullname));
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_FROM.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, account));

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

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

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
                self._logger.log(Level.SEVERE, traceback.format_exc())

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if skype_files.index(file) == 0:
                social_app.append("Skype".decode('UTF-8'))
#End Skype

#Extract gmail messages
        for file in gmail_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
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
                        mess_id = resultSet_messages.getString("messageId");                   
                        dialog_partner = resultSet_messages.getString("toAddresses");
                        author = resultSet_messages.getString("fromAddress");
                        date_sent = int(resultSet_messages.getString("dateSentMs"))/1000;
                        date_receive = int(resultSet_messages.getString("dateReceivedMs"))/1000;
                        text = resultSet_messages.getString("body");

                        subject = resultSet_messages.getString("subject");
                        snippet = resultSet_messages.getString("snippet");
                        attachment = resultSet_messages.getString("joinedAttachmentInfos");

                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from gmail (" + e.getMessage() + ")")

                    status_arr=[]
                    if  subject is not None and subject!="":
                        status_arr.append("Тема письма: \"".decode('UTF-8'))
                        status_arr.append(subject)
                        status_arr.append("\"; ".decode('UTF-8'))
                    if  snippet is not None and snippet!="":
                        status_arr.append("Фрагмент письма (снипет): \"".decode('UTF-8'))
                        status_arr.append(snippet)
                        status_arr.append("\"; ".decode('UTF-8'))
                    if  attachment is not None and attachment!="":
                        status_arr.append("Вложения: \"".decode('UTF-8'))
                        status_arr.append(attachment)
                        status_arr.append("\"; ".decode('UTF-8'))
                    status=' '.join(status_arr)

                    art = file.newArtifact(artID_gmail)                

                    art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, mess_id))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_SENT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date_sent))                
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_RCVD.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date_receive))

                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, author))
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, dialog_partner))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, text))

                    art.addAttribute(BlackboardAttribute(attID_status, IMDbIngestModuleFactory.moduleName, status))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            try:
                if 'resultSet_messages' in locals():
                    resultSet_messages.close()
                stmt.close()
                dbConn.close()
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error closing database", ex)
                self._logger.log(Level.SEVERE, traceback.format_exc())

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if gmail_files.index(file) == 0:
                social_app.append("Gmail".decode('UTF-8'))
#End gmail

#Extract AquaMail messages
        for file in aquamail_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
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
                        mess_id = resultSet_messages.getString("msg_id");                   
                        dialog_partner = resultSet_messages.getString("who_to");
                        author = resultSet_messages.getString("who_from");
                        date_sent = int(resultSet_messages.getString("when_date"))/1000;
                        text = resultSet_messages.getString("body_alt_content_utf8");

                        subject = resultSet_messages.getString("subject");
                        preview_attachments=resultSet_messages.getString("preview_attachments");
                        attachment = resultSet_messages.getString("joinedAttachmentInfos");                    
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from messages table (aquamail) (" + e.getMessage() + ")")

                    status_arr=[]
                    if  subject is not None and subject!="":
                        status_arr.append("Тема письма: \"".decode('UTF-8'))
                        status_arr.append(subject)
                        status_arr.append("\"; ".decode('UTF-8'))
                    if  preview_attachments is not None and preview_attachments!="":
                        status_arr.append("Вложения (имена файлов): \"".decode('UTF-8'))
                        status_arr.append(preview_attachments)
                    if  attachment is not None and attachment!="":
                        status_arr.append("\"; ".decode('UTF-8'))
                        status_arr.append("Вложения (путь к файлам): \"".decode('UTF-8'))
                        status_arr.append(attachment)
                        status_arr.append("\"; ".decode('UTF-8'))
                    status=' '.join(status_arr)

                    art = file.newArtifact(artID_aquamail)                

                    art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, mess_id))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME_SENT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date_sent))                

                    art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, author))
                    art.addAttribute(BlackboardAttribute(attID_reciever, IMDbIngestModuleFactory.moduleName, dialog_partner))

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, text))

                    art.addAttribute(BlackboardAttribute(attID_status, IMDbIngestModuleFactory.moduleName, status))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            try:
                if resultSet_messages in locals():
                    resultSet_messages.close()
                stmt.close()
                dbConn.close()
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error closing database", ex)
                self._logger.log(Level.SEVERE, traceback.format_exc())                

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if aquamail_files.index(file) == 0:
                social_app.append("Aquamail".decode('UTF-8'))
#End Aquamail               

#Extract odnoklassniki
        for file in odnoklassniki_files:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            #contacts
            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
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
                        user_id = resultSet_account_info.getString("uid");
                        first_name = resultSet_account_info.getString("first_name");
                        last_name = resultSet_account_info.getString("last_name");
                        username = resultSet_account_info.getString("login");
                        pic= resultSet_account_info.getString("uri_pic");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from contacts table (odnoclassniki) (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_contacts_odnoclassniki)                

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, first_name+" ".decode('UTF-8')+last_name))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, user_id))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, pic))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "Логин: ".decode('UTF-8')+username))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, "Авторизированный пользователь приложения \"Одноклассники\"".decode('UTF-8')))
                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            if 'resultSet_contacts' in locals():
                while resultSet_contacts.next():
                    try:
                        user_id = resultSet_contacts.getString("user_id");
                        first_name = resultSet_contacts.getString("user_first_name");
                        last_name = resultSet_contacts.getString("user_last_name");
                        user_name = resultSet_contacts.getString("user_name");
                        pic= resultSet_contacts.getString("user_avatar_url");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from contacts table (odnoclassniki) (" + e.getMessage() + ")")

                    art = file.newArtifact(artID_contacts_odnoclassniki)                

                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, first_name+" ".decode('UTF-8')+last_name))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_ID.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, user_id))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_USER_NAME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, user_name))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, pic))
                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            #messages
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
                        conversation_table=check_table.getString("name")
                    except:
                        self.log(Level.INFO, "Error to get data from tables info")
                        conversation_table=None

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
                            messages_id = resultSet_messages.getString("_id");
                            user_id = resultSet_messages.getString("author_id");
                            user_name = resultSet_messages.getString("user_name");
                            message = resultSet_messages.getString("message");
                            date = int(resultSet_messages.getString("date"))/1000;
                        except SQLException as e:
                            self.log(Level.INFO, "Error getting values from messages table (odnoclassniki) (" + e.getMessage() + ")")
                        art = file.newArtifact(artID_odnoclassniki)                

                        art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, messages_id))
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, date))
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, message))
                        art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, user_name+" (Идентификатор пользователя:   ".decode('UTF-8')+user_id+")".decode('UTF-8')))

                        IngestServices.getInstance().fireModuleDataEvent(
                            ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                            BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

                else:
                    while resultSet_messages.next():
                        try:
                            messages_id = resultSet_messages.getString("_id");
                            message_hex_str = resultSet_messages.getString("message_hex_str");
                            date = int(resultSet_messages.getString("_date"))/1000;
                            companion_hex_str = resultSet_messages.getString("companion_hex_data");
                        except SQLException as e:
                            self.log(Level.INFO, "Error getting values from messages table (odnoclassniki) (" + e.getMessage() + ")")

                        if message_hex_str[0:2]=="0A":
                            message_length=int(message_hex_str[2:4], 16)
                            start_message=4
                            array_to_find=wrap(message_hex_str,2)
                            start_point_next=array_to_find.index("12")
                            end_message=start_point_next*2
                            message=message_hex_str[start_message:end_message]
                            message=bytearray.fromhex(message).decode('utf-8')

                            full_length=len(message_hex_str)/2

                            if message_length>full_length:                    
                                array_to_find=wrap(message_hex_str,2)
                                start_point_id=array_to_find.index("12")*2+4
                                start_point_user=array_to_find.index("180148")*2+4
                            else:
                                string_to_find=message_hex_str[message_length*2+4:]
                                array_to_find=wrap(string_to_find,2)
                                start_point_id=array_to_find.index("12")*2+message_length*2+4+4
                                start_point_user_test=string_to_find.find("E0AE90")                   

                            end_point_id=start_point_id+(int(message_hex_str[(start_point_id-2):(start_point_id)], 16))*2
                            end_point_user=message_hex_str.rfind("E0AE90")
                            user_id=(message_hex_str[start_point_id:end_point_id])
                            user_id=bytearray.fromhex(user_id).decode('utf-8')
                            if start_point_user_test==-1:
                                user_name=" ".decode('utf-8')
                            else:
                                start_point_user=start_point_user_test+message_length*2+4+6
                                user_name=(message_hex_str[start_point_user:end_point_user])
                                user_name=bytearray.fromhex(user_name).decode('utf-8')
                        else:
                            start_point_id=4
                            end_point_id=start_point_id+(int(message_hex_str[(start_point_id-2):(start_point_id)], 16))*2
                            user_id=(message_hex_str[start_point_id:end_point_id])
                            user_id=bytearray.fromhex(user_id).decode('utf-8')
                            user_name=" ".decode('utf-8')

                        if tableFound:
                            conv_id_length=int(companion_hex_str[2:4], 16)
                            string_to_find=companion_hex_str[conv_id_length*2+4:]
                            array_to_find=wrap(string_to_find,2)
                            start_point_name=array_to_find.index("1A")*2+conv_id_length*2+8
                            start_point_id=array_to_find.index("0A")*2+conv_id_length*2+8
                            companion_length=int(companion_hex_str[start_point_name-2:start_point_name], 16)
                            id_length=int(companion_hex_str[start_point_id-2:start_point_id], 16)
                            end_point_name=start_point_name+companion_length*2
                            end_point_id=start_point_id+id_length*2
                            companion=companion_hex_str[start_point_name:end_point_name]
                            companion=bytearray.fromhex(companion).decode('utf-8')
                            companion_id=companion_hex_str[start_point_id:end_point_id]
                            companion_id=bytearray.fromhex(companion_id).decode('utf-8')

                        art = file.newArtifact(artID_odnoclassniki)                

                        art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, messages_id))
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, date))
                        art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                             IMDbIngestModuleFactory.moduleName, message))
                        art.addAttribute(BlackboardAttribute(attID_sender, IMDbIngestModuleFactory.moduleName, user_name+" (Идентификатор пользователя:   ".decode('UTF-8')+user_id+")".decode('UTF-8')))
                        if 'tableFound' in locals():
                            art.addAttribute(BlackboardAttribute(attID_companion, IMDbIngestModuleFactory.moduleName, companion+" (Идентификатор пользователя:   ".decode('UTF-8')+companion_id+")".decode('UTF-8')))

                        IngestServices.getInstance().fireModuleDataEvent(
                            ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                            BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

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
                self._logger.log(Level.SEVERE, traceback.format_exc())
                
            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)

            progressBar.progress(fileCount)
            if odnoklassniki_files.index(file) == 0:
                social_app.append("Одноклассники".decode('UTF-8'))

        for file in odnoklassniki_files_tam:		
            if self.context.isJobCancelled():
                return IngestModule.ProcessResult.OK

            self.log(Level.INFO, "Processing file: " + file.getName())
            fileCount += 1
            lclDbPath = os.path.join(Case.getCurrentCase().getTempDirectory(), str(file.getId()) + ".db")
            ContentUtils.writeToFile(file, File(lclDbPath))

            try: 
                Class.forName("org.sqlite.JDBC").newInstance()
                dbConn = DriverManager.getConnection("jdbc:sqlite:%s"  % lclDbPath)
                stmt = dbConn.createStatement()
            except SQLException as e:
                self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

            try:
                resultSet_messages = stmt.executeQuery("select messages.[msg_server_id], messages.[msg_time], messages.[msg_text], messages.[msg_sender], messages.[msg_attaches], messages.[msg_media_type], (SELECT hex(contacts.[ctt_data]) FROM contacts WHERE messages.[msg_chat_id]=contacts.[_id]) as [hex_data] from messages order by messages.[msg_time]".decode('UTF-8'))          
            except SQLException as e:
                self.log(Level.INFO, "Error querying database for odnoclassniki (" + e.getMessage() + ")")

            if 'resultSet_messages' in locals():
                while resultSet_messages.next():
                    try:
                        mess_id = resultSet_messages.getString("msg_server_id");
                        date = int(resultSet_messages.getString("msg_time"))/1000;
                        text = resultSet_messages.getString("msg_text");
                        msg_type = int(resultSet_messages.getString("msg_media_type"));
                        msg_sender = resultSet_messages.getString("msg_sender");
                        user_hex_str = resultSet_messages.getString("hex_data");
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from messages table (skype) (" + e.getMessage() + ")")

                    full_length=len(user_hex_str)/2
                    skip_length=int(user_hex_str[2:4], 16)

                    if skip_length>full_length:                    
                        array_to_find=wrap(user_hex_str,2)
                        start_point=array_to_find.index("0A")*2+4
                    else:
                        string_to_find=user_hex_str[skip_length*2+4:]
                        array_to_find=wrap(string_to_find,2)
                        start_point=array_to_find.index("0A")*2+skip_length*2+4+4                    

                    end_point=start_point+(int(user_hex_str[(start_point-2):(start_point)], 16))*2
                    user_name=(user_hex_str[start_point:end_point])
                    user_name=bytearray.fromhex(user_name).decode('utf-8')

                    art = file.newArtifact(artID_odnoclassniki)                

                    art.addAttribute(BlackboardAttribute(attID_nr, IMDbIngestModuleFactory.moduleName, mess_id))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, date))
                    art.addAttribute(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(), 
                                                         IMDbIngestModuleFactory.moduleName, text))
                    art.addAttribute(BlackboardAttribute(attID_companion, IMDbIngestModuleFactory.moduleName, user_name))

                    if msg_type == 0:
                        art.addAttribute(BlackboardAttribute(attID_status, 
                                                             IMDbIngestModuleFactory.moduleName, "Идентификатор отправителя: ".decode('UTF-8')+msg_sender))
                    else:
                        art.addAttribute(BlackboardAttribute(attID_status, 
                                                         IMDbIngestModuleFactory.moduleName, "Идентификатор отправителя: ".decode('UTF-8')+msg_sender+"; Передан мультимедийный контент (изображение, видео, звуковой файл и т.п.)".decode('UTF-8')))

                    IngestServices.getInstance().fireModuleDataEvent(
                        ModuleDataEvent(IMDbIngestModuleFactory.moduleName, 
                                        BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

            try:
                if 'resultSet_messages' in locals():
                    resultSet_messages.close()
                stmt.close()
                dbConn.close()
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error closing database", ex)
                self._logger.log(Level.SEVERE, traceback.format_exc())                

            try:
                os.remove(lclDbPath)
            except Exception as ex:
                self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
                
            progressBar.progress(fileCount)
            if odnoklassniki_files_tam.index(file) == 0:
                social_app.append("Одноклассники (tamtam_messages)".decode('UTF-8'))
            #End odnoklassniki
        all_social_app=', '.join(social_app)
        if len(social_app)>0:
            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                                  "IM SQliteDB Analyzer", "Обнаружены базы данных: %s ".decode('UTF-8') % all_social_app)
            IngestServices.getInstance().postMessage(message)
            self.log(Level.INFO, "Обнаружены базы данных программного обеспечения: ".decode('UTF-8') + all_social_app)
        else:
            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                                  "IM SQliteDB Analyzer", "Базы данных не обнаружены".decode('UTF-8'))
            IngestServices.getInstance().postMessage(message)
            self.log(Level.INFO, "Базы данных известного программного обеспечения не обнаружены".decode('UTF-8'))

        return IngestModule.ProcessResult.OK
