# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import binascii
import os
import json
import sre


def vk(self, progressBar, vk_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_vk1 = case.getSleuthkitCase().addArtifactType("TSK_CHATS_VK1", "ВКонтакте - сообщения".decode('UTF-8'))
    except:
        artID_vk1 = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VK1")
    try:
        artID_contact = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CONTACTS_VK", "ВКонтакте - контакты".decode('UTF-8'))
    except:
        artID_contact = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_VK")

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

    for file in vk_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        # Save the DB locally in the temp folder. use file id as name to reduce collisions
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        # Open the DB using JDBC
        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
            stmt2 = dbConn.createStatement()
            stmt3 = dbConn.createStatement()
        except SQLException as e:
            self.log(Level.INFO, "Could not open database file (not SQLite) " + file.getName() + " (" + e.getMessage() + ")")

        # Query the contacts table in the database and get all columns.
        try:
            resultSet = stmt.executeQuery("select messages.mid as ID, messages.peer as [peer], messages.[time] as [date], (select users.lastname || ' ' || users.firstname from users where users.uid=messages.sender) || ' (id_peer: '|| messages.sender || ')' as [Sender], case messages.sender=messages.peer when 0 then (select users.lastname || ' ' || users.firstname from users where users.uid=messages.peer) || ' (id_peer: ' || messages.peer || ')' else  (select users.lastname || ' ' || users.firstname || ' (id: ' || users.uid || ' - учетная запись мобильного телефона)' from users, messages where messages.peer!=messages.sender and users.uid=messages.sender) end as [User_who_recieved_messages], messages.text as [text], hex(messages.attachments) as attachments from  messages order by messages.[time]".decode('UTF-8'))
            db_case = 1
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for vk messages (" + e.getMessage() + ")")

        try:
            resultSet = stmt.executeQuery("select account_name, record_data as json_string, (select profile.record_data from profile where messages.account_name=profile.profile_id) as profile, (select messages_profiles.record_data from messages_profiles where messages.dialog_id=messages_profiles.dialog_id) as chat_info from messages order by message_date".decode('UTF-8'))
            db_case = 2
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for vk messages in json (" + e.getMessage() + ")")    
        
        try:
            resultSet_contacts = stmt2.executeQuery("select users.uid as [uid], users.lastname as [lastname], users.firstname as [firstname], users.photo_small as [photo], users.[is_friend] as [friend], birthdays.bday || '.' || birthdays.bmonth || '.' || birthdays.byear as [birthdays] from users left join birthdays on birthdays.uid=users.uid ORDER BY users.lastname")
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for vk contacts 1 (" + e.getMessage() + ")")

        try:
            resultSet_contacts = stmt3.executeQuery("select users.uid as [uid], users.lastname as [lastname], users.firstname as [firstname], users.photo_small as [photo], users.[is_friend] as [friend], users.bday || '.' || users.bmonth || '.' || users.byear as [birthdays] from users ORDER BY users.lastname")
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for vk contacts 2 (" + e.getMessage() + ")")
            
        try:
            resultSet_contacts = stmt2.executeQuery("select friends.friend_id as [uid], friends.friend_last_name as [lastname], friends.friend_first_name as [firstname], friends.friend_avatar as [photo], 1 as [friend], friends.friend_bdate as [birthdays] from friends ORDER BY friends.friend_last_name")
        except SQLException as e:
            self.log(Level.INFO, "Error querying database for vk contacts 3 (" + e.getMessage() + ")")

        # Cycle through each row and create artifacts
        if 'resultSet' in locals():
            while resultSet.next():
                if db_case == 1:
                    try:
                        ID = resultSet.getString("ID")
                        date = resultSet.getInt("date")
                        sender = resultSet.getString("Sender")
                        if resultSet.getString("User_who_recieved_messages") is None:
                            reciever = "Идентификатор беседы (чата): ".decode('UTF-8')+resultSet.getString("peer")
                            mess = resultSet.getString("text")
                            tmp_string = binascii.unhexlify(resultSet.getString("attachments"))
                        else:
                            reciever = resultSet.getString("User_who_recieved_messages")
                            mess = resultSet.getString("text")
                            tmp_string = binascii.unhexlify(resultSet.getString("attachments"))
                        if tmp_string:
                            status = "Приложение: ".decode('UTF-8')+tmp_string.decode("UTF-8", 'ignore');
                        else:
                            status = " "
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from vk message table (" + e.getMessage() + ")")
                elif db_case == 2:
                    try:
                        json_data = json.loads(resultSet.getString("json_string"))
                        profile = json.loads(resultSet.getString("profile"))
                        chat_info_json_str = resultSet.getString("chat_info")
                        if chat_info_json_str:
                            chat_info = json.loads(chat_info_json_str)
                        
                        account_id = resultSet.getString("account_name")
                        # self.log(Level.INFO, (profile.get("first_name").encode('utf-8')).decode('UTF-8'))
                        profile_user_info = str(profile.get("first_name").encode('utf-8')).decode('utf-8') + " " + str(profile.get("last_name").encode('utf-8')).decode('UTF-8') + " (" + account_id + ")" 

                        
                        ID = str(json_data.get("id"))
                        date = int(json_data.get("date"))
                        message_type = int(json_data.get("out"))
                        mess = (json_data.get("body").encode("utf-8")).decode('utf-8')
                        message_user_id = str(json_data.get("user_id"))
                        if message_user_id != account_id:
                            # user_info_string = str(chat_info.get(message_user_id))
                            if chat_info_json_str:
                                other_user_json = chat_info.get(message_user_id)
                                # self.log(Level.INFO, str(other_user_json))
                                other_user = str(other_user_json.get("f").encode("UTF-8")).decode('utf-8') + " " + str(other_user_json.get("g").encode("UTF-8")).decode('utf-8') + " (" + message_user_id + ")"
                            else:
                                other_user = "Идентификатор пользователя: ".decode('UTF-8') + message_user_id 
                        if message_type == 0:
                            status = "Входящее".decode('UTF-8')
                            sender = other_user
                            reciever = profile_user_info
                            if int(json_data.get("read_state")) == 1:
                                status = status + "; Прочитано".decode('UTF-8')
                            else:
                                status = status + "; Не прочитано".decode('UTF-8')
                        else:                        
                            status = "Исходящее".decode('UTF-8')
                            sender = profile_user_info
                            reciever = other_user
                    except SQLException as e:
                        self.log(Level.INFO, "Error getting values from vk message table in json (" + e.getMessage() + ")")
                # Make an artifact on the blackboard, TSK_CONTACT and give it attributes for each of the fields
                # art = file.newArtifact(BlackboardArtifact.ARTIFACT_TYPE.TSK_VKdb)
                art = file.newArtifact(artID_vk1)

                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_MESSAGE_TYPE,
                                                     imdbIngestModuleFactory.moduleName, "VK"))
                art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, ID))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date))
                art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, sender))
                art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, reciever))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, mess))
                art.addAttribute(blackboardAttribute(attID_status, imdbIngestModuleFactory.moduleName, status))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))
        if 'resultSet_contacts' in locals():
            while resultSet_contacts.next():
                try:
                    user = resultSet_contacts.getString("firstname")+" "+resultSet_contacts.getString("lastname")+" ("+resultSet_contacts.getString("uid")+")"
                    photo = resultSet_contacts.getString("photo")
                    friend = resultSet_contacts.getInt("friend")
                    birthdays = resultSet_contacts.getString("birthdays")
                    if friend == 1 and birthdays is not None:
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
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, user))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, photo))
                art.addAttribute(blackboardAttribute(attID_status, imdbIngestModuleFactory.moduleName, status))
                # Fire an event to notify the UI and others that there are new artifacts
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        IM_sqlitedb_android.IMDbIngestModule.set_count(self, 1)
        progressBar.progress(IM_sqlitedb_android.IMDbIngestModule.get_count(self))
        if vk_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных: VK (ВКонтакте)".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "VK (ВКонтакте)")

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
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
