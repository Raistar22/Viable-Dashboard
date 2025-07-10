/**
 * WebApp.gs - Enhanced web interface with proper error handling (FIXED)
 */

/**
 * Main entry point for web app with enhanced routing
 */
function doGet(e) {
    try {
      // Perform health check
      const health = performSystemHealthCheck();
      if (health.status === 'unhealthy') {
        return createErrorPage('System is currently unavailable. Please check configuration.');
      }
      
      const page = e.parameter.page || 'dashboard';
      const client = e.parameter.client;
      
      switch (page) {
        case 'dashboard':
          return getDashboardPage();
        case 'client':
          if (!client) {
            return createErrorPage('Client parameter is required for client page');
          }
          return getClientPage(client);
        case 'setup':
          return getSetupPage();
        default:
          return getDashboardPage();
      }
      
    } catch (error) {
      errorLog('Error in doGet', error);
      return createErrorPage(`Application error: ${error.message}`);
    }
  }
  
  /**
   * Enhanced POST handler with proper request validation
   */
  function doPost(e) {
    try {
      // Validate request
      if (!e.postData || !e.postData.contents) {
        return createErrorResponse('Invalid request: No data provided');
      }
      
      let data;
      try {
        data = JSON.parse(e.postData.contents);
      } catch (parseError) {
        return createErrorResponse('Invalid request: Malformed JSON');
      }
      
      if (!data.action) {
        return createErrorResponse('Invalid request: Action is required');
      }
      
      debugLog('Processing POST request', { action: data.action });
      
      // Route to appropriate handler
      switch (data.action) {
        case 'addClient':
          return addClientAction(data);
        case 'getClients':
          return getClientsAction();
        case 'deleteClient':
          return deleteClientAction(data);
        case 'processGmail':
          return processGmailAction(data);
        case 'processAI':
          return processAIAction(data);
        case 'processBuffer':
          return processBufferAction(data);
        case 'moveToFlow':
          return moveToFlowAction(data);
        case 'validateReactivation':
          return validateReactivationAction(data);
        case 'getSystemStatus':
          return getSystemStatusAction();
        default:
          return createErrorResponse(`Unknown action: ${data.action}`);
      }
      
    } catch (error) {
      errorLog('Error in doPost', error);
      return createErrorResponse(`Server error: ${error.message}`);
    }
  }
  
  /**
   * Enhanced client creation with comprehensive validation and rollback
   */
  function addClientAction(data) {
    try {
      // Validate input data
      if (!data.clientName || !data.gmailLabel) {
        return createErrorResponse('Client name and Gmail label are required');
      }
      
      const clientName = data.clientName.trim();
      const gmailLabel = data.gmailLabel.trim();
      const parentFolderId = data.parentFolderId ? data.parentFolderId.trim() : null;
      
      // Additional validation
      if (clientName.length < 2) {
        return createErrorResponse('Client name must be at least 2 characters long');
      }
      
      if (gmailLabel.length < 2) {
        return createErrorResponse('Gmail label must be at least 2 characters long');
      }
      
      // Validate Gmail label format
      if (!gmailLabel.startsWith(SYSTEM_CONFIG.GMAIL.LABEL_PREFIX)) {
        warnLog(`Gmail label doesn't follow convention: ${gmailLabel}`);
      }
      
      infoLog(`Web app: Starting addClientAction for: ${clientName}`);
      
      // Use atomic client creation
      const result = addClientWithAtomicTransaction(clientName, gmailLabel, parentFolderId);
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          message: result.message,
          clientData: {
            clientName: clientName,
            gmailLabel: gmailLabel,
            rootFolderId: result.client.rootFolderId,
            spreadsheetId: result.client.spreadsheetId,
            status: result.client.status
          }
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in addClientAction', error);
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: false,
          error: error.message,
          code: error.code || 'UNKNOWN_ERROR'
        }))
        .setMimeType(ContentService.MimeType.JSON);
    }
  }
  
  /**
   * Direct add client function for google.script.run calls (FIXED)
   */
  function addClientDirect(data) {
    try {
      // Validate input
      if (!data || !data.clientName || !data.gmailLabel) {
        return {
          success: false,
          error: 'Client name and Gmail label are required'
        };
      }
      
      const clientName = data.clientName.trim();
      const gmailLabel = data.gmailLabel.trim();
      const parentFolderId = data.parentFolderId ? data.parentFolderId.trim() : null;
      
      infoLog(`Direct call: Starting addClientDirect for: ${clientName}`);
      
      // Use atomic client creation
      const result = addClientWithAtomicTransaction(clientName, gmailLabel, parentFolderId);
      
      // Return plain JavaScript object (not ContentService)
      return {
        success: true,
        message: result.message,
        clientData: {
          clientName: clientName,
          gmailLabel: gmailLabel,
          rootFolderId: result.client.rootFolderId,
          spreadsheetId: result.client.spreadsheetId,
          status: result.client.status
        }
      };
        
    } catch (error) {
      errorLog('Error in addClientDirect', error);
      
      // Return plain JavaScript object with error (not ContentService)
      return {
        success: false,
        error: error.message,
        code: error.code || 'UNKNOWN_ERROR'
      };
    }
  }
  
  /**
   * Get clients action with pagination support
   */
  function getClientsAction() {
    try {
      const clients = getActiveClients();
      const clientsData = clients.map(client => ({
        name: client.name,
        gmailLabel: client.gmailLabel,
        status: client.status,
        rootFolderId: client.rootFolderId,
        spreadsheetId: client.spreadsheetId,
        createdAt: client.createdAt,
        lastModified: client.lastModified
      }));
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          clients: clientsData,
          count: clientsData.length
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in getClientsAction', error);
      return createErrorResponse(`Error retrieving clients: ${error.message}`);
    }
  }
  
  /**
   * Delete client action with validation
   */
  function deleteClientAction(data) {
    try {
      if (!data.clientName) {
        return createErrorResponse('Client name is required for deletion');
      }
      
      const clientName = data.clientName.trim();
      
      // For safety, we'll deactivate instead of actually deleting
      const result = deactivateClient(clientName);
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          message: `Client '${clientName}' has been deactivated`,
          result: result
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in deleteClientAction', error);
      return createErrorResponse(`Error deactivating client: ${error.message}`);
    }
  }
  
  /**
   * Process Gmail action with enhanced error handling
   */
  function processGmailAction(data) {
    try {
      let result;
      
      if (data.clientName) {
        validateInput(data.clientName, 'string', 'Client name');
        result = processClientGmailByName(data.clientName);
      } else {
        result = processAllClientsGmail();
      }
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          result: result
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in processGmailAction', error);
      return createErrorResponse(`Gmail processing failed: ${error.message}`);
    }
  }
  
  /**
   * Process AI action with rate limiting awareness
   */
  function processAIAction(data) {
    try {
      let result;
      
      if (data.clientName) {
        validateInput(data.clientName, 'string', 'Client name');
        result = processClientDocumentsWithAI(data.clientName);
      } else {
        result = processAllClientsWithAI();
      }
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          result: result
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in processAIAction', error);
      return createErrorResponse(`AI processing failed: ${error.message}`);
    }
  }
  
  /**
   * Process buffer changes action (handles both deletions and reactivations)
   */
  function processBufferAction(data) {
    try {
      if (!data.clientName) {
        return createErrorResponse('Client name is required for buffer processing');
      }
      
      validateInput(data.clientName, 'string', 'Client name');
      
      const result = processBufferChanges(data.clientName);
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          result: result
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in processBufferAction', error);
      return createErrorResponse(`Buffer processing failed: ${error.message}`);
    }
  }
  
  /**
   * Move to flow action (final to inflow/outflow)
   */
  function moveToFlowAction(data) {
    try {
      if (!data.clientName) {
        return createErrorResponse('Client name is required for flow processing');
      }
      
      validateInput(data.clientName, 'string', 'Client name');
      
      const result = moveFilesToInflowOutflow(data.clientName);
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          result: result
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in moveToFlowAction', error);
      return createErrorResponse(`Flow processing failed: ${error.message}`);
    }
  }
  
  /**
   * Validate reactivation action
   */
  function validateReactivationAction(data) {
    try {
      if (!data.clientName) {
        return createErrorResponse('Client name is required for reactivation validation');
      }
      
      validateInput(data.clientName, 'string', 'Client name');
      
      const result = validatePendingReactivations(data.clientName);
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          result: result
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in validateReactivationAction', error);
      return createErrorResponse(`Reactivation validation failed: ${error.message}`);
    }
  }
  
  /**
   * Get system status action
   */
  function getSystemStatusAction() {
    try {
      const status = getSystemStatus();
      
      return ContentService
        .createTextOutput(JSON.stringify({
          success: true,
          status: status
        }))
        .setMimeType(ContentService.MimeType.JSON);
        
    } catch (error) {
      errorLog('Error in getSystemStatusAction', error);
      return createErrorResponse(`System status check failed: ${error.message}`);
    }
  }
  
  /**
   * Get dashboard page with enhanced error handling
   */
  function getDashboardPage() {
    try {
      const template = HtmlService.createTemplateFromFile('dashboard');
      
      // Get clients safely
      let clients = [];
      let systemStatus = {};
      
      try {
        clients = getActiveClients();
      } catch (error) {
        warnLog('Error loading clients for dashboard', error);
        clients = [];
      }
      
      try {
        systemStatus = getSystemStatus();
      } catch (error) {
        warnLog('Error loading system status for dashboard', error);
        systemStatus = {
          systemHealth: 'degraded',
          totalClients: 0,
          activeClients: 0,
          issues: ['Error loading system status']
        };
      }
      
      template.clients = clients;
      template.systemStatus = systemStatus;
      
      return template.evaluate()
        .setTitle('FinTech Automation Dashboard')
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        
    } catch (error) {
      errorLog('Error creating dashboard page', error);
      return createErrorPage('Failed to load dashboard: ' + error.message);
    }
  }
  
  /**
   * Get client-specific page with comprehensive data
   */
  function getClientPage(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        return createErrorPage(`Client '${clientName}' not found`);
      }
      
      const template = HtmlService.createTemplateFromFile('client');
      
      // Load client data safely
      let statistics = {};
      let gmailStats = {};
      let aiStats = {};
      let sheetStats = {};
      let pendingChanges = {};
      
      try {
        statistics = getClientStatistics(clientName);
      } catch (error) {
        warnLog(`Error loading client statistics for ${clientName}`, error);
        statistics = { error: error.message };
      }
      
      try {
        gmailStats = getGmailProcessingStats(clientName);
      } catch (error) {
        warnLog(`Error loading Gmail stats for ${clientName}`, error);
        gmailStats = { error: error.message };
      }
      
      try {
        aiStats = getAIProcessingStats(clientName);
      } catch (error) {
        warnLog(`Error loading AI stats for ${clientName}`, error);
        aiStats = { error: error.message };
      }
      
      try {
        sheetStats = getSheetStatistics(clientName);
      } catch (error) {
        warnLog(`Error loading sheet stats for ${clientName}`, error);
        sheetStats = { error: error.message };
      }
      
      try {
        pendingChanges = getPendingBufferChanges(clientName);
      } catch (error) {
        warnLog(`Error loading pending changes for ${clientName}`, error);
        pendingChanges = { error: error.message, hasChanges: false };
      }
      
      template.client = client;
      template.statistics = statistics;
      template.gmailStats = gmailStats;
      template.aiStats = aiStats;
      template.sheetStats = sheetStats;
      template.pendingChanges = pendingChanges;
      
      return template.evaluate()
        .setTitle(`${clientName} - Client Dashboard`)
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        
    } catch (error) {
      errorLog('Error creating client page', error);
      return createErrorPage('Failed to load client page: ' + error.message);
    }
  }
  
  /**
   * Get setup page for new client creation
   */
  function getSetupPage() {
    try {
      const template = HtmlService.createTemplateFromFile('setup');
      
      // Get Gmail labels safely
      let gmailLabels = [];
      try {
        gmailLabels = getGmailLabelsForClient();
      } catch (error) {
        warnLog('Error loading Gmail labels for setup', error);
        gmailLabels = [];
      }
      
      template.gmailLabels = gmailLabels;
      
      return template.evaluate()
        .setTitle('Setup New Client')
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        
    } catch (error) {
      errorLog('Error creating setup page', error);
      return createErrorPage('Failed to load setup page: ' + error.message);
    }
  }
  
  /**
   * Create error page with consistent styling
   */
  function createErrorPage(errorMessage) {
    try {
      const template = HtmlService.createTemplate(`
        <!DOCTYPE html>
        <html>
        <head>
          <title>Error - FinTech Automation</title>
          <meta charset="UTF-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <style>
            body { 
              font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
              margin: 0; 
              padding: 40px;
              background: #f9fafb;
              color: #111827;
            }
            .error-container {
              max-width: 600px;
              margin: 0 auto;
              background: white;
              border-radius: 12px;
              box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
              overflow: hidden;
            }
            .error-header {
              background: linear-gradient(135deg, #ef4444, #dc2626);
              color: white;
              padding: 2rem;
              text-align: center;
            }
            .error-header h1 {
              margin: 0;
              font-size: 1.5rem;
              font-weight: 600;
            }
            .error-content {
              padding: 2rem;
            }
            .error-message {
              background: #fee2e2;
              border: 1px solid #fca5a5;
              color: #991b1b;
              padding: 1rem;
              border-radius: 6px;
              margin-bottom: 1.5rem;
            }
            .error-actions {
              display: flex;
              gap: 1rem;
              justify-content: center;
            }
            .btn {
              padding: 0.75rem 1.5rem;
              border: none;
              border-radius: 6px;
              font-weight: 500;
              text-decoration: none;
              cursor: pointer;
              transition: all 0.2s;
            }
            .btn-primary {
              background: #2563eb;
              color: white;
            }
            .btn-primary:hover {
              background: #1d4ed8;
            }
            .btn-secondary {
              background: #6b7280;
              color: white;
            }
            .btn-secondary:hover {
              background: #4b5563;
            }
          </style>
        </head>
        <body>
          <div class="error-container">
            <div class="error-header">
              <h1>🚨 Application Error</h1>
            </div>
            <div class="error-content">
              <div class="error-message">
                <strong>Error:</strong> <?= errorMessage ?>
              </div>
              <div class="error-actions">
                <a href="?" class="btn btn-primary">← Back to Dashboard</a>
                <button onclick="location.reload()" class="btn btn-secondary">🔄 Refresh Page</button>
              </div>
            </div>
          </div>
        </body>
        </html>
      `);
      
      template.errorMessage = errorMessage || 'An unknown error occurred';
      
      return template.evaluate()
        .setTitle('Error - FinTech Automation')
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
        
    } catch (templateError) {
      errorLog('Error creating error page template', templateError);
      
      // Fallback to simple HTML
      return HtmlService.createHtmlOutput(`
        <html>
          <body style="font-family: Arial, sans-serif; padding: 40px; text-align: center;">
            <h1 style="color: #dc2626;">Application Error</h1>
            <p style="color: #991b1b; background: #fee2e2; padding: 20px; border-radius: 5px; display: inline-block;">
              ${errorMessage || 'An unknown error occurred'}
            </p>
            <br><br>
            <a href="?" style="background: #2563eb; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">
              ← Back to Dashboard
            </a>
          </body>
        </html>
      `)
      .setTitle('Error - FinTech Automation')
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
    }
  }
  
  /**
   * Create standardized error response for API calls
   */
  function createErrorResponse(message, code = 'ERROR') {
    try {
      return ContentService
        .createTextOutput(JSON.stringify({
          success: false,
          error: message,
          code: code,
          timestamp: getCurrentTimestamp()
        }))
        .setMimeType(ContentService.MimeType.JSON);
    } catch (error) {
      errorLog('Error creating error response', error);
      return ContentService
        .createTextOutput('{"success":false,"error":"Internal server error"}')
        .setMimeType(ContentService.MimeType.JSON);
    }
  }
  
  /**
   * Get system status with comprehensive health checks
   */
  function getSystemStatus() {
    try {
      const health = performSystemHealthCheck();
      const clients = getActiveClients();
      
      const status = {
        systemHealth: health.status,
        totalClients: clients.length,
        activeClients: clients.filter(c => c.status === SYSTEM_CONFIG.STATUS.ACTIVE).length,
        lastUpdate: getCurrentTimestamp(),
        pendingChanges: 0,
        issues: health.issues || []
      };
      
      // Count pending changes across all clients
      let totalPendingChanges = 0;
      clients.forEach(client => {
        try {
          const pendingChanges = getPendingBufferChanges(client.name);
          if (pendingChanges.hasChanges) {
            totalPendingChanges += pendingChanges.summary.totalChanges;
          }
        } catch (error) {
          debugLog(`Error getting pending changes for client: ${client.name}`, error);
        }
      });
      
      status.pendingChanges = totalPendingChanges;
      
      return status;
      
    } catch (error) {
      errorLog('Error getting system status', error);
      return {
        systemHealth: 'error',
        error: error.message,
        lastUpdate: getCurrentTimestamp(),
        totalClients: 0,
        activeClients: 0,
        pendingChanges: 0,
        issues: ['System status check failed']
      };
    }
  }
  
  /**
   * Get comprehensive client statistics
   */
  function getClientStatistics(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const gmailStats = getGmailProcessingStats(clientName);
      const aiStats = getAIProcessingStats(clientName);
      const sheetStats = getSheetStatistics(clientName);
      const pendingChanges = getPendingBufferChanges(clientName);
      
      return {
        gmail: gmailStats,
        ai: aiStats,
        sheets: sheetStats,
        pendingChanges: pendingChanges,
        lastUpdate: getCurrentTimestamp()
      };
      
    } catch (error) {
      errorLog(`Error getting client statistics for: ${clientName}`, error);
      return {
        error: error.message,
        lastUpdate: getCurrentTimestamp()
      };
    }
  }
  
  /**
   * Validate pending reactivations
   */
  function validatePendingReactivations(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const pendingChanges = getPendingBufferChanges(clientName);
      const validation = {
        isValid: true,
        errors: [],
        warnings: [],
        reactivations: pendingChanges.reactivations
      };
      
      // Check each reactivation for potential issues
      pendingChanges.reactivations.forEach((reactivation, index) => {
        try {
          // Check if file still exists in Drive
          const fileId = extractFileIdFromUrl(reactivation.fileUrl);
          if (fileId) {
            DriveApp.getFileById(fileId);
          } else {
            validation.errors.push(`Invalid file URL for: ${reactivation.originalFilename}`);
            validation.isValid = false;
          }
        } catch (error) {
          validation.errors.push(`File not found in Drive: ${reactivation.originalFilename}`);
          validation.isValid = false;
        }
        
        // Check if file needs re-processing
        if (!hasValidAIData(reactivation)) {
          validation.warnings.push(`File will need AI processing: ${reactivation.originalFilename}`);
        }
      });
      
      return validation;
      
    } catch (error) {
      errorLog(`Error validating reactivations for client: ${clientName}`, error);
      return {
        isValid: false,
        errors: [`Validation failed: ${error.message}`],
        warnings: [],
        reactivations: []
      };
    }
  }
  
  /**
   * Include HTML files with error handling
   */
  function include(filename) {
    try {
      return HtmlService.createHtmlOutputFromFile(filename).getContent();
    } catch (error) {
      errorLog(`Error including file: ${filename}`, error);
      return `<!-- Error loading ${filename}: ${error.message} -->`;
    }
  }
  
  /**
   * Manual trigger functions for testing and debugging
   */
  function testGmailProcessing(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      return processClientGmailByName(clientName);
    } catch (error) {
      errorLog('Error in testGmailProcessing', error);
      throw error;
    }
  }
  
  function testAIProcessing(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      return processClientDocumentsWithAI(clientName);
    } catch (error) {
      errorLog('Error in testAIProcessing', error);
      throw error;
    }
  }
  
  function testBufferProcessing(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      return processBufferChanges(clientName);
    } catch (error) {
      errorLog('Error in testBufferProcessing', error);
      throw error;
    }
  }
  
  function testFlowProcessing(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      return moveFilesToInflowOutflow(clientName);
    } catch (error) {
      errorLog('Error in testFlowProcessing', error);
      throw error;
    }
  }
  
  /**
   * Get processing queue status for monitoring
   */
  function getProcessingQueueStatus() {
    try {
      const clients = getActiveClients();
      const queueStatus = [];
      
      clients.forEach(client => {
        try {
          const stats = getClientStatistics(client.name);
          queueStatus.push({
            clientName: client.name,
            pendingGmail: stats.gmail.activeFiles || 0,
            pendingAI: stats.ai.pendingProcessing || 0,
            pendingFlow: stats.sheets.final || 0,
            pendingDeletions: stats.pendingChanges.summary?.deletionCount || 0,
            pendingReactivations: stats.pendingChanges.summary?.reactivationCount || 0,
            lastProcessed: stats.gmail.lastProcessed || null,
            status: 'healthy'
          });
        } catch (error) {
          queueStatus.push({
            clientName: client.name,
            error: error.message,
            status: 'error'
          });
        }
      });
      
      return {
        success: true,
        queue: queueStatus,
        lastUpdate: getCurrentTimestamp()
      };
      
    } catch (error) {
      errorLog('Error getting processing queue status', error);
      return {
        success: false,
        error: error.message,
        lastUpdate: getCurrentTimestamp()
      };
    }
  }
  
  /**
   * Comprehensive system diagnostic
   */
  function runSystemDiagnostic() {
    try {
      const diagnostic = {
        timestamp: getCurrentTimestamp(),
        overall: 'healthy',
        components: {},
        issues: [],
        recommendations: []
      };
      
      // Check system health
      const health = performSystemHealthCheck();
      diagnostic.components.systemHealth = health;
      
      if (health.status !== 'healthy') {
        diagnostic.overall = health.status;
        diagnostic.issues.push(...health.issues);
      }
      
      // Check clients
      try {
        const clients = getAllClients();
        diagnostic.components.clientCount = clients.length;
        
        let healthyClients = 0;
        for (const client of clients) {
          try {
            const validation = validateClientConfiguration(client.name);
            if (validation.isValid) {
              healthyClients++;
            } else {
              diagnostic.issues.push(`Client ${client.name}: ${validation.errors.join(', ')}`);
            }
          } catch (error) {
            diagnostic.issues.push(`Client ${client.name}: Validation failed`);
          }
        }
        
        diagnostic.components.healthyClients = healthyClients;
        
        if (healthyClients < clients.length) {
          diagnostic.overall = 'degraded';
        }
        
      } catch (error) {
        diagnostic.components.clientCheck = 'failed';
        diagnostic.issues.push('Cannot access client data');
        diagnostic.overall = 'degraded';
      }
      
      // Add recommendations
      if (diagnostic.issues.length > 0) {
        diagnostic.recommendations.push('Review and fix identified issues');
      }
      
      if (diagnostic.components.clientCount === 0) {
        diagnostic.recommendations.push('Add your first client to get started');
      }
      
      return diagnostic;
      
    } catch (error) {
      errorLog('Error running system diagnostic', error);
      return {
        timestamp: getCurrentTimestamp(),
        overall: 'error',
        error: error.message,
        components: {},
        issues: ['Diagnostic failed to run'],
        recommendations: ['Check system logs and configuration']
      };
    }
  }