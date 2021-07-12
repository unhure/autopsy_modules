# -*- coding: utf-8 -*-
from java.util.logging import Level
from java.io import File
from java.lang import Class
from java.sql import DriverManager
from java.sql import SQLException
import os
import IM_sqlitedb_android


def kate(self,  progressBar, kate_files):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_vk_kate = case.getSleuthkitCase().addArtifactType("TSK_CHATS_VK2", "Kate Mobile (ВКонтакте) - сообщения".decode('UTF-8'))
    except:
        artID_vk_kate = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_VK2")
    try:
        artID_contact_kate = case.getSleuthkitCase().addArtifactType("TSK_CHATS_CONTACTS_KATE", "Kate Mobile (ВКонтакте) - контакты".decode('UTF-8'))
    except:
        artID_contact_kate = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_CONTACTS_KATE")
    try:
        artID_wall = case.getSleuthkitCase().addArtifactType("TSK_CHATS_WALL", "Kate Mobile (ВКонтакте) - стена".decode('UTF-8'))
    except:
        artID_wall = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_WALL")

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
       
    for file in kate_files:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))
        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
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
                    mess_id = resultSet.getString("message_id")
                    date = resultSet.getInt("date")
                    sender = resultSet.getString("sender_name")
                    mess = resultSet.getString("text")
                    nickname = resultSet.getString("nickname")
                    birthday = resultSet.getString("rec_birthday")
                    name_mess = resultSet.getString("name_mess")
                    info_arr = []
                    info_arr.append(resultSet.getString("reciever_name"))
                    if nickname != "" and nickname is not None:
                        info_arr.append(" (логин: ".decode('UTF-8'))
                        info_arr.append(nickname)
                        info_arr.append(") ".decode('UTF-8'))
                    if birthday != "" and birthday is not None:
                        info_arr.append(", День рождения: ".decode('UTF-8'))
                        info_arr.append(birthday)
                    reciever = ''.join(info_arr)
                    status_arr = []
                    if name_mess != "" and name_mess is not None:
                        status_arr.append("Название переписки: \"".decode('UTF-8'))
                        status_arr.append(name_mess)
                        status_arr.append("\"; ".decode('UTF-8'))
                        status_arr.append("Статус сообщения: ".decode('UTF-8'))
                        status_arr.append(resultSet.getString("status1"))
                    status = ''.join(status_arr)
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from kate message table (" + e.getMessage() + ")")

                art = file.newArtifact(artID_vk_kate)
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_MESSAGE_TYPE,
                                                     imdbIngestModuleFactory.moduleName, "Kate Mobile"))
                art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, mess_id))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date))
                if resultSet.getInt("status2") == 0:
                    art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, reciever))
                    art.addAttribute(blackboardAttribute(attID_reciever, imdbIngestModuleFactory.moduleName, sender))
                else:
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
                    name = resultSet_contacts.getString("id")
                    photo_link = resultSet_contacts.getString("photo")
                    mobile_phone = resultSet_contacts.getString("mobile_phone")
                    home_phone = resultSet_contacts.getString("home_phone")
                    nickname = resultSet_contacts.getString("nickname")
                    birthdate = resultSet_contacts.getString("birthdate")
                    status = resultSet_contacts.getString("status")
                    status_arr = []
                    if nickname is not None and nickname != "":
                        status_arr.append("Псевдоним пользователя: \"".decode('UTF-8'))
                        status_arr.append(nickname)
                        status_arr.append("\"; ".decode('UTF-8'))
                    if birthdate is not None and birthdate != "":
                        status_arr.append("Указанный пользователем день рождения: ".decode('UTF-8'))
                        status_arr.append(birthdate)
                        status_arr.append("; ".decode('UTF-8'))
                    if status is not None and status != "":
                        status_arr.append("Указанный пользователем статус: ".decode('UTF-8'))
                        status_arr.append(status)
                        status = ''.join(status_arr)
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from kate contacts table (" + e.getMessage() + ")")

                art = file.newArtifact(artID_contact_kate)
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, name))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_URL.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, photo_link))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_MOBILE.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, mobile_phone))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER_HOME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, home_phone))
                art.addAttribute(blackboardAttribute(attID_status, imdbIngestModuleFactory.moduleName, status))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        # wall
        if 'resultSet_wall' in locals():
            while resultSet_wall.next():
                try:
                    post_id = resultSet_wall.getString("_id")
                    user = resultSet_wall.getString("user")
                    date = resultSet_wall.getInt("date")
                    text = resultSet_wall.getString("text")
                except SQLException as e:
                    self.log(Level.INFO, "Error getting values from kate wall table (" + e.getMessage() + ")")

                art = file.newArtifact(artID_wall)
                art.addAttribute(blackboardAttribute(attID_nr, imdbIngestModuleFactory.moduleName, post_id))
                art.addAttribute(blackboardAttribute(attID_sender, imdbIngestModuleFactory.moduleName, user))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, date))
                art.addAttribute(blackboardAttribute(blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                                                     imdbIngestModuleFactory.moduleName, text))
                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(imdbIngestModuleFactory.moduleName,
                                                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE, None))

        file_count = IM_sqlitedb_android.IMDbIngestModule.get_count(self) + 1
        IM_sqlitedb_android.IMDbIngestModule.set_count(self, file_count)
        progressBar.progress(file_count)
        if kate_files.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                                                                      imdbIngestModuleFactory.moduleName, "Обнаружены базы данных:  Kate Mobile (ВКонтакте)".decode('UTF-8'))
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(self, "Kate Mobile (ВКонтакте)")

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
            self._logger.log(Level.SEVERE, IM_sqlitedb_android.traceback.format_exc())

        try:
            os.remove(lclDbPath)
        except Exception as ex:
            self._logger.log(Level.SEVERE, "Error delete database from temp folder", ex)
