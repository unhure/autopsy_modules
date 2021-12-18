# -*- coding: UTF-8 -*-
from javax.swing import JCheckBox
from javax.swing import JLabel
from javax.swing import JTextArea
from javax.swing import BoxLayout
from javax.swing import BorderFactory
from javax.swing import JPanel
from javax.swing import JScrollPane
from javax.swing import JComponent
# from javax.swing import JFrame
# from java.awt import BorderLayout
# from javax.swing import JToolBar
# from java.awt import GridLayout
# from javax.swing import JList
# from java.awt.event import KeyListener
# from java.awt.event import KeyEvent
# from java.awt.event import KeyAdapter
# from javax.swing.event import DocumentEvent
# from javax.swing.event import DocumentListener
# from java.lang import Class
# from java.lang import System
# from java.sql  import DriverManager, SQLException
# from java.util.logging import Level
# from java.io import File
# from org.sleuthkit.autopsy.datamodel import ContentUtils


from java.util.logging import Level
import inspect
import traceback
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
from org.sleuthkit.autopsy.ingest import GenericIngestModuleJobSettings
from org.sleuthkit.autopsy.ingest import IngestModuleIngestJobSettingsPanel
from org.sleuthkit.autopsy.ingest import IngestMessage
from org.sleuthkit.autopsy.ingest import IngestJobContext
from org.sleuthkit.autopsy.ingest import IngestServices
from org.sleuthkit.autopsy.ingest import ModuleDataEvent
from org.sleuthkit.autopsy.coreutils import Logger
from org.sleuthkit.autopsy.coreutils import PlatformUtil
from org.sleuthkit.autopsy.casemodule import Case
from org.sleuthkit.autopsy.casemodule.services import Services
from org.sleuthkit.autopsy.casemodule.services import FileManager
from org.sleuthkit.autopsy.casemodule.services import Blackboard
# from org.sleuthkit.autopsy.casemodule.services import Blackboard
# Factory that defines the name and details of the module and allows Autopsy
# to create instances of the modules that will do the analysis.

from vk import vk
from kate import kate
from viber_calls import viber_calls
from viber_messages import viber_messages
from skype_app import skype
from gmail import gmail
from aquamail import aquamail
from odnoklassniki_app import odnoklassniki
from odnoklassniki_tamtam import odnoklassniki_tam
from search_for_tables import search_for_tables
from rugram import rugram

class IMDbIngestModuleFactory(IngestModuleFactoryAdapter):

    moduleName = "IM SQliteDB Analyzer"

    def getModuleDisplayName(self):
        return self.moduleName

    def getModuleDescription(self):
        return "Копирует информацию из некоторых баз данных SQlite".decode('UTF-8')

    def getModuleVersionNumber(self):
        return "1.0"

    def isDataSourceIngestModuleFactory(self):
        return True

    def createDataSourceIngestModule(self, ingestOptions):
        return IMDbIngestModule()
    
    def getDefaultIngestJobSettings(self):
        return GenericIngestModuleJobSettings()
    
    def hasIngestJobSettingsPanel(self):
        return True
    
    def getIngestJobSettingsPanel(self, settings):
#        if not isinstance(settings, GenericIngestModuleJobSettings):
#            raise IllegalArgumentException("Expected settings argument to be instance of GenericIngestModuleJobSettings")
        self.settings = settings
        return IMDbUISettingsPanel(self.settings)
    
    def createDataSourceIngestModule(self, ingestOptions):
        return IMDbIngestModule(self.settings)

# Data Source-level ingest module.  One gets created per data source.
class IMDbIngestModule(DataSourceIngestModule):

    _logger = Logger.getLogger(IMDbIngestModuleFactory.moduleName)

    def log(self, level, msg):
        self._logger.logp(level, self.__class__.__name__, inspect.stack()[1][3], msg)

    def __init__(self, settings, fileCount=0, social_app=[]):
        self.context = None
        self.local_settings = settings
        self.social_app = social_app
        self.fileCount = fileCount
        self.List_Of_Tables = []

    def get_count(self):
        return self.fileCount

    def get_social_app_list(self):
        return self.social_app

    def set_count(self, num):
        self.fileCount += num

    def set_social_app_list(self, app):
        self.social_app.append(app.decode('UTF-8'))

    # Where any setup and configuration is done
    # 'context' is an instance of org.sleuthkit.autopsy.ingest.IngestJobContext.
    def startUp(self, context):
        self.context = context
        # Throw an IngestModule.IngestModuleException exception if there was a problem setting up
        # raise IngestModuleException("Oh No!")

        if self.local_settings.getSetting('Flag') == 'true':
            Table_List = self.local_settings.getSetting('tableList').split(',')
            for tbl in Table_List:
                self.List_Of_Tables.append(str(tbl).strip('\n').replace(' ',''))
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
        skype_files = fileManager.findFiles(dataSource, "%.db", "com.skype.raider")
        gmail_files = fileManager.findFiles(dataSource, "mailstore.%.db", "com.google.android.%")
        aquamail_files = fileManager.findFiles(dataSource, "Messages.%", "org.kman.AquaMail")
        odnoklassniki_files = fileManager.findFiles(dataSource, "odnklassniki.db")
        odnoklassniki_files_tam = fileManager.findFiles(dataSource, "tamtam_messages")
	rugram_files = fileManager.findFiles(dataSource, "cache4.db", "com.rugram.%")
        if self.local_settings.getSetting('Flag') == 'true':
            all_db_files = fileManager.findFiles(dataSource, "%.db")
            sqlite_files = fileManager.findFiles(dataSource, "%.sqlite")
        else:
            all_db_files, sqlite_files = [],[]
        
        numFiles = len(vk_files) + len(kate_files) + len(viber_calls_files) + \
                len(viber_messages_files) + len(skype_files) + len(gmail_files) + \
                len(odnoklassniki_files) + len(odnoklassniki_files_tam) + \
                len(all_db_files) + len(sqlite_files) + len(rugram_files)
        self.log(Level.INFO, "Found " + str(numFiles) + " files")
        progressBar.switchToDeterminate(numFiles)

        if self.context.isJobCancelled():
            return IngestModule.ProcessResult.OK

        if vk_files:
            vk(self, progressBar, vk_files)

        if kate_files:
            kate(self, progressBar, kate_files)

        if viber_calls_files:
            viber_calls(self, progressBar, viber_calls_files)

        if viber_messages_files:
            viber_messages(self, progressBar, viber_messages_files)

        if skype_files:
            skype(self, progressBar, skype_files)

        if gmail_files:
            gmail(self, progressBar, gmail_files)

        if aquamail_files:
            aquamail(self,  progressBar, aquamail_files)

        if odnoklassniki_files:
            odnoklassniki(self,  progressBar, odnoklassniki_files)

        if odnoklassniki_files_tam:
            odnoklassniki_tam(self,  progressBar, odnoklassniki_files_tam)

        if rugram_files:
            rugram(self, progressBar, rugram_files)
            
        if self.local_settings.getSetting('Flag') == 'true':
            search_for_tables(self, progressBar, all_db_files, self.List_Of_Tables)
            search_for_tables(self, progressBar, sqlite_files, self.List_Of_Tables)

        all_social_app = ', '.join(self.social_app)
        if len(self.social_app) > 0:
            self.log(Level.INFO, "Обнаружены базы данных программного обеспечения: ".decode('UTF-8') + all_social_app)
        else:
            message = IngestMessage.createMessage(IngestMessage.MessageType.DATA,
                                                  IMDbIngestModuleFactory.moduleName, "Поддерживаемых баз данных программного обеспечения не обнаружено".decode('UTF-8'))
            IngestServices.getInstance().postMessage(message)
            self.log(Level.INFO, "Поддерживаемых баз данных не обнаружено".decode('UTF-8'))

        return IngestModule.ProcessResult.OK

class IMDbUISettingsPanel(IngestModuleIngestJobSettingsPanel):
    def __init__(self, settings):
        self.local_settings = settings
        self.initComponents()
        self.customizeComponents()
        self.area.setText("message, call, user, chat")
        self.local_settings.setSetting('tableList', self.area.getText())
    
    def checkBoxEvent(self, event):
        if self.checkbox.isSelected():
            self.local_settings.setSetting('Flag', 'true')
            self.local_settings.setSetting('tableList', self.area.getText())
        else:
            self.local_settings.setSetting('Flag', 'false')
    
    def initComponents(self):
        self.setLayout(BoxLayout(self, BoxLayout.Y_AXIS))
        self.setAlignmentX(JComponent.LEFT_ALIGNMENT)
        self.panel1 = JPanel()
        self.panel1.setLayout(BoxLayout(self.panel1, BoxLayout.Y_AXIS))
        self.panel1.setAlignmentY(JComponent.LEFT_ALIGNMENT)
        self.checkbox = JCheckBox("Искать таблицы?".decode('UTF-8'), actionPerformed=self.checkBoxEvent)
        self.label0 = JLabel(" ")
        self.label1 = JLabel("Введите названия интересуемых таблиц".decode('UTF-8'))
        self.label2 = JLabel("через запятую, после чего установите флажок".decode('UTF-8'))
        self.label3 = JLabel(" ")
        self.panel1.add(self.checkbox)
        self.panel1.add(self.label0)
        self.panel1.add(self.label1)
        self.panel1.add(self.label2)
        self.panel1.add(self.label3)
        self.add(self.panel1)
        self.local_settings.setSetting('Flag', 'false')

        self.area = JTextArea(5,25)
        self.area.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0))
        self.pane = JScrollPane()
        self.pane.getViewport().add(self.area)
        self.add(self.pane)

    def customizeComponents(self):
        self.checkbox.setSelected(self.local_settings.getSetting('Flag') == 'true')
        self.area.setText(self.local_settings.getSetting('tableList'))
        
    def getSettings(self):
        return self.local_settings