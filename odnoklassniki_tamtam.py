# -*- coding: utf-8 -*-
import IM_sqlitedb_android
from java.lang import Class
from java.sql import DriverManager
from java.util.logging import Level
from java.io import File
from java.sql import SQLException
import os
from textwrap import wrap


def odnoklassniki_tam(self, progressBar, odnoklassniki_files_tam):
    blackboardAttribute = IM_sqlitedb_android.BlackboardAttribute
    imdbIngestModuleFactory = IM_sqlitedb_android.IMDbIngestModuleFactory
    case = IM_sqlitedb_android.Case.getCurrentCase()

    try:
        artID_odnoclassniki = case.getSleuthkitCase().addArtifactType(
            "TSK_CHATS_OK", "Одноклассники - сообщения".decode("UTF-8")
        )
    except:
        artID_odnoclassniki = case.getSleuthkitCase().getArtifactTypeID("TSK_CHATS_OK")
    try:
        artID_contacts_odnoclassniki = case.getSleuthkitCase().addArtifactType(
            "TSK_CHATS_CONTACTS_ODNOCLASSNIKI",
            "Одноклассники - контакты".decode("UTF-8"),
        )
    except:
        artID_contacts_odnoclassniki = case.getSleuthkitCase().getArtifactTypeID(
            "TSK_CHATS_CONTACTS_ODNOCLASSNIKI"
        )

    try:
        attID_nr = case.getSleuthkitCase().addArtifactAttributeType(
            "TSK_MESS_ID",
            blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING,
            "Идентификатор сообщения".decode("UTF-8"),
        )
    except:
        attID_nr = case.getSleuthkitCase().getAttributeType("TSK_MESS_ID")
    try:
        attID_status = case.getSleuthkitCase().addArtifactAttributeType(
            "TSK_MESS_STATUS",
            blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING,
            "Дополнительная информация".decode("UTF-8"),
        )
    except:
        attID_status = case.getSleuthkitCase().getAttributeType("TSK_MESS_STATUS")
    try:
        attID_companion = case.getSleuthkitCase().addArtifactAttributeType(
            "TSK_MESS_COMPANION",
            blackboardAttribute.TSK_BLACKBOARD_ATTRIBUTE_VALUE_TYPE.STRING,
            "Собеседник".decode("UTF-8"),
        )
    except:
        attID_companion = case.getSleuthkitCase().getAttributeType("TSK_MESS_COMPANION")

    for file in odnoklassniki_files_tam:
        self.log(Level.INFO, "Processing file: " + file.getName())
        lclDbPath = os.path.join(case.getTempDirectory(), str(file.getId()) + ".db")
        IM_sqlitedb_android.ContentUtils.writeToFile(file, File(lclDbPath))

        try:
            Class.forName("org.sqlite.JDBC").newInstance()
            dbConn = DriverManager.getConnection("jdbc:sqlite:%s" % lclDbPath)
            stmt = dbConn.createStatement()
        except SQLException as e:
            self.log(
                Level.INFO,
                "Could not open database file (not SQLite) "
                + file.getName()
                + " ("
                + e.getMessage()
                + ")",
            )

        try:
            resultSet_messages = stmt.executeQuery(
                "select messages.[msg_server_id], messages.[msg_time], messages.[msg_text], messages.[msg_sender], messages.[msg_attaches], messages.[msg_media_type], (SELECT hex(contacts.[ctt_data]) FROM contacts WHERE messages.[msg_chat_id]=contacts.[_id]) as [hex_data] from messages order by messages.[msg_time]".decode(
                    "UTF-8"
                )
            )
        except SQLException as e:
            self.log(
                Level.INFO,
                "Error querying database for odnoclassniki (" + e.getMessage() + ")",
            )

        if "resultSet_messages" in locals():
            while resultSet_messages.next():
                try:
                    mess_id = resultSet_messages.getString("msg_server_id")
                    date = int(resultSet_messages.getString("msg_time")) / 1000
                    text = resultSet_messages.getString("msg_text")
                    msg_type = int(resultSet_messages.getString("msg_media_type"))
                    msg_sender = resultSet_messages.getString("msg_sender")
                    user_hex_str = resultSet_messages.getString("hex_data")
                except SQLException as e:
                    self.log(
                        Level.INFO,
                        "Error getting values from messages table (odnoklassniki_tam) ("
                        + e.getMessage()
                        + ")",
                    )

                full_length = len(user_hex_str) / 2
                skip_length = int(user_hex_str[2:4], 16)

                if skip_length > full_length:
                    array_to_find = wrap(user_hex_str, 2)
                    start_point = array_to_find.index("0A") * 2 + 4
                else:
                    string_to_find = user_hex_str[skip_length * 2 + 4 :]
                    array_to_find = wrap(string_to_find, 2)
                    start_point = (
                        array_to_find.index("0A") * 2 + skip_length * 2 + 4 + 4
                    )

                end_point = (
                    start_point
                    + (int(user_hex_str[(start_point - 2) : (start_point)], 16)) * 2
                )
                user_name = user_hex_str[start_point:end_point]
                user_name = bytearray.fromhex(user_name).decode("utf-8")

                art = file.newArtifact(artID_odnoclassniki)
                art.addAttribute(
                    blackboardAttribute(
                        attID_nr, imdbIngestModuleFactory.moduleName, mess_id
                    )
                )
                art.addAttribute(
                    blackboardAttribute(
                        blackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME.getTypeID(),
                        imdbIngestModuleFactory.moduleName,
                        date,
                    )
                )
                art.addAttribute(
                    blackboardAttribute(
                        blackboardAttribute.ATTRIBUTE_TYPE.TSK_TEXT.getTypeID(),
                        imdbIngestModuleFactory.moduleName,
                        text,
                    )
                )
                art.addAttribute(
                    blackboardAttribute(
                        attID_companion, imdbIngestModuleFactory.moduleName, user_name
                    )
                )

                if msg_type == 0:
                    art.addAttribute(
                        blackboardAttribute(
                            attID_status,
                            imdbIngestModuleFactory.moduleName,
                            "Идентификатор отправителя: ".decode("UTF-8") + msg_sender,
                        )
                    )
                else:
                    art.addAttribute(
                        blackboardAttribute(
                            attID_status,
                            imdbIngestModuleFactory.moduleName,
                            "Идентификатор отправителя: ".decode("UTF-8")
                            + msg_sender
                            + "; Передан мультимедийный контент (изображение, видео, звуковой файл и т.п.)".decode(
                                "UTF-8"
                            ),
                        )
                    )

                IM_sqlitedb_android.IngestServices.getInstance().fireModuleDataEvent(
                    IM_sqlitedb_android.ModuleDataEvent(
                        imdbIngestModuleFactory.moduleName,
                        IM_sqlitedb_android.BlackboardArtifact.ARTIFACT_TYPE.TSK_MESSAGE,
                        None,
                    )
                )

        try:
            if "resultSet_messages" in locals():
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

        IM_sqlitedb_android.IMDbIngestModule.set_count(self, 1)
        progressBar.progress(IM_sqlitedb_android.IMDbIngestModule.get_count(self))
        if odnoklassniki_files_tam.index(file) == 0:
            message = IM_sqlitedb_android.IngestMessage.createMessage(
                IM_sqlitedb_android.IngestMessage.MessageType.DATA,
                imdbIngestModuleFactory.moduleName,
                "Обнаружены базы данных: Одноклассники (tamtam_messages)".decode(
                    "UTF-8"
                ),
            )
            IM_sqlitedb_android.IngestServices.getInstance().postMessage(message)
            IM_sqlitedb_android.IMDbIngestModule.set_social_app_list(
                self, "Одноклассники (tamtam_messages)"
            )
