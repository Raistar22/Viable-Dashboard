/**
 * Config.gs - Master Configuration for FinTech Automation System (FIXED)
 */

// Debug mode - set to true for detailed logging
const DEBUG_MODE = true;

// System constants
const SYSTEM_CONFIG = {
  // Gmail settings
  GMAIL: {
    LABEL_PREFIX: 'client-',
    BATCH_SIZE: 50,
    MAX_ATTACHMENTS_PER_RUN: 100,
    MESSAGE_ID_CACHE_SIZE: 10000
  },
  
  // Drive settings
  DRIVE: {
    FOLDER_STRUCTURE: {
      ACCRUALS: 'Accruals',
      BILLS_AND_INVOICES: 'Bills and Invoices',
      BUFFER: 'Buffer',
      MONTHS: 'Months',
      INFLOW: 'Inflow',
      OUTFLOW: 'Outflow',
      SPREADSHEETS: 'Spreadsheets'
    },
    MAX_FILE_SIZE: 25 * 1024 * 1024, // 25MB
    MAX_FILENAME_LENGTH: 100,
    ALLOWED_MIME_TYPES: [
      'application/pdf',
      'image/jpeg',
      'image/png', 
      'image/gif',
      'image/webp',
      'image/bmp',
      'image/tiff',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'text/csv',
      'text/plain'
    ]
  },
  
  // Sheet settings
  SHEETS: {
    BUFFER_SHEET_NAME: 'Buffer',
    FINAL_SHEET_NAME: 'Final',
    INFLOW_SHEET_NAME: 'Inflow',
    OUTFLOW_SHEET_NAME: 'Outflow',
    
    // Column mappings - Fixed order
    BUFFER_COLUMNS: [
      'Original File Name',
      'Changed File Name', 
      'File URL',
      'File ID',
      'Message ID',
      'Invoice Number',
      'Status',
      'Reason',
      'Email Subject',
      'Email Sender',
      'Date Added',
      'Last Modified',
      'Processing Attempts'
    ],
    
    FINAL_COLUMNS: [
      'File Name',
      'Unique File ID',
      'Drive File ID',
      'File URL',
      'Message ID',
      'Email Subject',
      'Email Sender',
      'Inflow/Outflow Status',
      'Date',
      'Vendor Name',
      'Invoice Number',
      'Amount',
      'Document Type',
      'AI Confidence',
      'Processing Date',
      'Last Modified'
    ],
    
    FLOW_COLUMNS: [
      'File Name',
      'Unique File ID',
      'Drive File ID',
      'File URL',
      'Message ID',
      'Email Subject',
      'Email Sender',
      'Date',
      'Vendor Name',
      'Invoice Number',
      'Amount',
      'Document Type',
      'AI Confidence',
      'Processing Date',
      'Moved Date'
    ]
  },
  
  // AI settings
  AI: {
    MODEL: 'gemini-1.5-flash',
    MAX_RETRIES: 3,
    TIMEOUT: 60000, // Increased timeout
    CONFIDENCE_THRESHOLD: 0.7,
    MAX_FILE_SIZE_FOR_AI: 10 * 1024 * 1024, // 10MB for AI processing
    
    // Enhanced prompt template
    PROMPTS: {
      DOCUMENT_ANALYSIS: `
        You are a financial document analysis expert. Analyze this document and extract the following information with high accuracy:
        
        1. Document Date (YYYY-MM-DD format) - Look for invoice date, bill date, or document date
        2. Vendor/Company Name - The company or person issuing this document
        3. Invoice/Document Number - Any reference number, invoice number, or bill number
        4. Total Amount (numerical value only) - The main amount due or paid
        5. Document Type - classify as: invoice, receipt, bill, statement, contract, other
        6. Transaction Type - determine if this represents:
           - "inflow" (money coming IN to the business - customer payments, sales, income)
           - "outflow" (money going OUT of the business - bills, expenses, purchases)
        7. Confidence Level (0.0 to 1.0) - Your confidence in the accuracy of the extraction
        
        Important guidelines:
        - For transaction type: invoices TO customers = inflow, bills FROM vendors = outflow
        - Use YYYY-MM-DD format for dates only
        - Extract only numerical values for amounts (no currency symbols)
        - Be conservative with confidence - use lower values if uncertain
        - If information is unclear or missing, use empty string for text fields and 0 for numerical fields
        
        Return ONLY valid JSON in this exact format (no other text):
        {
          "date": "YYYY-MM-DD",
          "vendorName": "vendor name",
          "invoiceNumber": "invoice number",
          "amount": "123.45",
          "documentType": "invoice|receipt|bill|statement|contract|other",
          "transactionType": "inflow|outflow",
          "confidence": 0.95
        }
      `
    }
  },
  
  // Processing settings
  PROCESSING: {
    MAX_CONCURRENT_OPERATIONS: 3, // Reduced for stability
    RETRY_DELAY: 2000,
    BATCH_DELAY: 1500, // Increased delay
    CLEANUP_OLDER_THAN_DAYS: 30,
    MAX_PROCESSING_ATTEMPTS: 3,
    OPERATION_TIMEOUT: 300000 // 5 minutes
  },
  
  // Status constants
  STATUS: {
    ACTIVE: 'Active',
    DELETED: 'Deleted', 
    PROCESSING: 'Processing',
    COMPLETED: 'Completed',
    ERROR: 'Error',
    PENDING: 'Pending',
    FAILED: 'Failed',
    INFLOW: 'inflow',
    OUTFLOW: 'outflow'
  },
  
  // Error codes
  ERROR_CODES: {
    DUPLICATE_CLIENT: 'DUPLICATE_CLIENT',
    INVALID_INPUT: 'INVALID_INPUT',
    PERMISSION_DENIED: 'PERMISSION_DENIED',
    API_LIMIT_EXCEEDED: 'API_LIMIT_EXCEEDED',
    FILE_NOT_FOUND: 'FILE_NOT_FOUND',
    PROCESSING_FAILED: 'PROCESSING_FAILED',
    SYSTEM_ERROR: 'SYSTEM_ERROR'
  }
};

/**
 * Get Gemini API key from script properties with validation
 */
function getGeminiApiKey() {
  try {
    const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
    if (!apiKey || apiKey.trim() === '') {
      throw createError(
        SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT,
        'GEMINI_API_KEY not found in script properties. Please add it in Project Settings > Script Properties.'
      );
    }
    return apiKey.trim();
  } catch (error) {
    errorLog('Error getting Gemini API key', error);
    throw error;
  }
}

/**
 * Get master configuration sheet ID with validation
 */
function getMasterConfigSheetId() {
  try {
    const sheetId = PropertiesService.getScriptProperties().getProperty('MASTER_CONFIG_SHEET_ID');
    if (!sheetId || sheetId.trim() === '') {
      throw createError(
        SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT,
        'MASTER_CONFIG_SHEET_ID not found in script properties. Please add it in Project Settings > Script Properties.'
      );
    }
    
    // Validate sheet access
    try {
      SpreadsheetApp.openById(sheetId.trim());
    } catch (accessError) {
      throw createError(
        SYSTEM_CONFIG.ERROR_CODES.PERMISSION_DENIED,
        `Cannot access master config sheet: ${sheetId}. Please check permissions.`
      );
    }
    
    return sheetId.trim();
  } catch (error) {
    errorLog('Error getting master config sheet ID', error);
    throw error;
  }
}

/**
 * Enhanced error creation with consistent structure
 */
function createError(code, message, details = null) {
  const error = new Error(message);
  error.code = code;
  error.timestamp = getCurrentTimestamp();
  if (details) {
    error.details = details;
  }
  return error;
}

/**
 * Enhanced logging functions with structured data
 */
function debugLog(message, data = null) {
  if (DEBUG_MODE) {
    const timestamp = getCurrentTimestamp();
    const logEntry = {
      level: 'DEBUG',
      timestamp: timestamp,
      message: message,
      data: data
    };
    console.log(`[DEBUG ${timestamp}] ${message}`);
    if (data) {
      console.log(JSON.stringify(data, null, 2));
    }
  }
}

function errorLog(message, error = null) {
  const timestamp = getCurrentTimestamp();
  const logEntry = {
    level: 'ERROR',
    timestamp: timestamp,
    message: message,
    error: error ? {
      message: error.message,
      code: error.code || 'UNKNOWN',
      stack: error.stack
    } : null
  };
  console.error(`[ERROR ${timestamp}] ${message}`);
  if (error) {
    console.error('Error details:', error);
    if (error.stack) {
      console.error('Stack trace:', error.stack);
    }
  }
}

function infoLog(message, data = null) {
  const timestamp = getCurrentTimestamp();
  const logEntry = {
    level: 'INFO',
    timestamp: timestamp,
    message: message,
    data: data
  };
  console.log(`[INFO ${timestamp}] ${message}`);
  if (data) {
    console.log(JSON.stringify(data, null, 2));
  }
}

function warnLog(message, data = null) {
  const timestamp = getCurrentTimestamp();
  console.warn(`[WARN ${timestamp}] ${message}`);
  if (data) {
    console.warn(JSON.stringify(data, null, 2));
  }
}

/**
 * Generate truly unique file ID with collision detection
 */
function generateUniqueId() {
  const uuid = Utilities.getUuid();
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substr(2, 9);
  return `${uuid}_${timestamp}_${random}`;
}

/**
 * Enhanced date formatting with validation
 */
function formatDateForFilename(date) {
  try {
    if (!date) return '';
    
    const d = date instanceof Date ? date : new Date(date);
    if (isNaN(d.getTime())) {
      warnLog('Invalid date for filename formatting', date);
      return '';
    }
    
    return d.toISOString().split('T')[0]; // YYYY-MM-DD format
  } catch (error) {
    errorLog('Error formatting date for filename', error);
    return '';
  }
}

/**
 * Enhanced filename cleaning with length validation
 */
function cleanFilename(filename) {
  try {
    if (!filename || typeof filename !== 'string') return '';
    
    // Remove or replace invalid characters
    let cleaned = filename
      .trim()
      .replace(/[<>:"/\\|?*\x00-\x1f]/g, '_') // Invalid file name chars
      .replace(/[^\x20-\x7E]/g, '_') // Non-ASCII chars
      .replace(/\.+$/, '') // Trailing dots
      .replace(/\s+/g, '_') // Multiple spaces to single underscore
      .replace(/_+/g, '_') // Multiple underscores to single
      .replace(/^_|_$/g, ''); // Leading/trailing underscores
    
    // Ensure length limit
    if (cleaned.length > SYSTEM_CONFIG.DRIVE.MAX_FILENAME_LENGTH) {
      cleaned = cleaned.substring(0, SYSTEM_CONFIG.DRIVE.MAX_FILENAME_LENGTH);
    }
    
    // Ensure not empty
    if (cleaned === '') {
      cleaned = 'unnamed_file';
    }
    
    return cleaned;
  } catch (error) {
    errorLog('Error cleaning filename', error);
    return 'unnamed_file';
  }
}

/**
 * Enhanced amount validation with better parsing
 */
function isValidAmount(amount) {
  try {
    if (amount === null || amount === undefined || amount === '') return false;
    
    const parsed = parseFloat(amount);
    return !isNaN(parsed) && isFinite(parsed) && parsed >= 0;
  } catch (error) {
    return false;
  }
}

/**
 * Enhanced amount parsing with multiple currency support
 */
function parseAmount(amountStr) {
  try {
    if (!amountStr) return '0';
    
    // Convert to string and clean
    let cleaned = amountStr.toString()
      .trim()
      .replace(/[$₹€£¥¢₦₵₡₨₩₪₫₽₼₺]/g, '') // Common currency symbols
      .replace(/[,\s]/g, '') // Remove commas and spaces
      .replace(/[()]/g, '') // Remove parentheses
      .replace(/[^\d.-]/g, ''); // Keep only digits, dots, and minus
    
    // Handle negative amounts in parentheses format
    const isNegative = amountStr.includes('(') && amountStr.includes(')');
    
    // Ensure only one decimal point
    const dotCount = (cleaned.match(/\./g) || []).length;
    if (dotCount > 1) {
      // Keep only the last decimal point
      const lastDotIndex = cleaned.lastIndexOf('.');
      cleaned = cleaned.substring(0, lastDotIndex).replace(/\./g, '') + cleaned.substring(lastDotIndex);
    }
    
    const parsed = parseFloat(cleaned);
    if (isNaN(parsed)) return '0';
    
    return (isNegative ? -Math.abs(parsed) : parsed).toString();
  } catch (error) {
    errorLog('Error parsing amount', error);
    return '0';
  }
}

/**
 * Get current timestamp in ISO format
 */
function getCurrentTimestamp() {
  return new Date().toISOString();
}

/**
 * Enhanced sleep function with validation
 */
function sleep(milliseconds) {
  try {
    if (!milliseconds || milliseconds < 0) return;
    Utilities.sleep(Math.min(milliseconds, 300000)); // Max 5 minutes
  } catch (error) {
    errorLog('Error in sleep function', error);
  }
}

/**
 * Enhanced file ID extraction with multiple URL formats
 */
function extractFileIdFromUrl(url) {
  try {
    if (!url || typeof url !== 'string') return null;
    
    // Multiple regex patterns for different Google Drive URL formats
    const patterns = [
      /\/file\/d\/([a-zA-Z0-9-_]+)/,
      /id=([a-zA-Z0-9-_]+)/,
      /folders\/([a-zA-Z0-9-_]+)/,
      /document\/d\/([a-zA-Z0-9-_]+)/,
      /spreadsheets\/d\/([a-zA-Z0-9-_]+)/
    ];
    
    for (const pattern of patterns) {
      const match = url.match(pattern);
      if (match && match[1]) {
        return match[1];
      }
    }
    
    return null;
  } catch (error) {
    errorLog('Error extracting file ID from URL', error);
    return null;
  }
}

/**
 * Enhanced retry function with exponential backoff and better error handling
 */
function retryWithBackoff(fn, maxRetries = 3, baseDelay = 1000, context = '') {
  if (typeof fn !== 'function') {
    throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, 'Function parameter is required for retry');
  }
  
  let lastError;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      debugLog(`Retry attempt ${attempt}/${maxRetries}${context ? ` (${context})` : ''}`);
      return fn();
    } catch (error) {
      lastError = error;
      
      if (attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt - 1) + Math.random() * 1000; // Jitter
        warnLog(`Attempt ${attempt} failed, retrying in ${delay}ms`, {
          error: error.message,
          context: context
        });
        sleep(delay);
      }
    }
  }
  
  errorLog(`All retry attempts failed${context ? ` (${context})` : ''}`, lastError);
  throw lastError;
}

/**
 * Validate input parameters
 */
function validateInput(value, type, fieldName, required = true) {
  if (required && (value === null || value === undefined || value === '')) {
    throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `${fieldName} is required`);
  }
  
  if (value === null || value === undefined || value === '') {
    return true; // Allow empty if not required
  }
  
  switch (type) {
    case 'string':
      if (typeof value !== 'string') {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `${fieldName} must be a string`);
      }
      break;
    case 'number':
      if (typeof value !== 'number' || isNaN(value)) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `${fieldName} must be a number`);
      }
      break;
    case 'email':
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(value)) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `${fieldName} must be a valid email address`);
      }
      break;
    default:
      break;
  }
  
  return true;
}

/**
 * Safe column index lookup
 */
function getColumnIndex(headers, columnName) {
  try {
    if (!Array.isArray(headers)) {
      throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, 'Headers must be an array');
    }
    
    const index = headers.indexOf(columnName);
    if (index === -1) {
      warnLog(`Column '${columnName}' not found in headers`, headers);
    }
    return index;
  } catch (error) {
    errorLog('Error getting column index', error);
    return -1;
  }
}

/**
 * Safe data access with validation
 */
function safeGetCellValue(row, index, defaultValue = '') {
  try {
    if (!Array.isArray(row) || index < 0 || index >= row.length) {
      return defaultValue;
    }
    
    const value = row[index];
    return value !== null && value !== undefined ? value : defaultValue;
  } catch (error) {
    return defaultValue;
  }
}

/**
 * Validate MIME type
 */
function isValidMimeType(mimeType) {
  try {
    if (!mimeType || typeof mimeType !== 'string') return false;
    return SYSTEM_CONFIG.DRIVE.ALLOWED_MIME_TYPES.includes(mimeType.toLowerCase());
  } catch (error) {
    return false;
  }
}

/**
 * Validate date string
 */
function isValidDate(dateString) {
  try {
    if (!dateString || typeof dateString !== 'string') return false;
    
    const regex = /^\d{4}-\d{2}-\d{2}$/;
    if (!regex.test(dateString)) return false;
    
    const date = new Date(dateString + 'T00:00:00Z');
    return date instanceof Date && !isNaN(date.getTime());
  } catch (error) {
    return false;
  }
}

/**
 * System health check
 */
function performSystemHealthCheck() {
  const health = {
    timestamp: getCurrentTimestamp(),
    status: 'healthy',
    checks: {},
    issues: []
  };
  
  try {
    // Check Gemini API key
    try {
      getGeminiApiKey();
      health.checks.geminiApi = 'ok';
    } catch (error) {
      health.checks.geminiApi = 'failed';
      health.issues.push('Gemini API key not configured');
      health.status = 'degraded';
    }
    
    // Check master config sheet
    try {
      getMasterConfigSheetId();
      health.checks.masterConfig = 'ok';
    } catch (error) {
      health.checks.masterConfig = 'failed';
      health.issues.push('Master config sheet not accessible');
      health.status = 'degraded';
    }
    
    // Check script properties
    try {
      const properties = PropertiesService.getScriptProperties().getProperties();
      health.checks.scriptProperties = 'ok';
      health.checks.propertyCount = Object.keys(properties).length;
    } catch (error) {
      health.checks.scriptProperties = 'failed';
      health.issues.push('Cannot access script properties');
      health.status = 'degraded';
    }
    
    if (health.issues.length > 2) {
      health.status = 'unhealthy';
    }
    
  } catch (error) {
    errorLog('Error performing health check', error);
    health.status = 'unhealthy';
    health.error = error.message;
  }
  
  return health;
}