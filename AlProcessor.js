/**
 * AIProcessor.gs - Complete Enhanced AI Document Processing (FIXED)
 */

/**
 * Process documents with AI for a specific client with comprehensive error handling
 */
function processClientDocumentsWithAI(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      infoLog(`Starting AI processing for client: ${clientName}`);
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      // Validate client configuration
      const validation = validateClientConfiguration(client.name);
      if (!validation.isValid) {
        throw createError(
          SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT,
          `Client configuration invalid: ${validation.errors.join(', ')}`
        );
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const bufferSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME);
      const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
      
      // Get active files from buffer sheet (includes reactivated files)
      const activeFiles = getActiveFilesFromBuffer(bufferSheet);
      
      if (activeFiles.length === 0) {
        return {
          success: true,
          message: 'No active files found for AI processing',
          totalFiles: 0,
          processed: 0,
          skipped: 0,
          reactivated: 0,
          errors: 0,
          results: []
        };
      }
      
      const results = {
        totalFiles: activeFiles.length,
        processed: 0,
        skipped: 0,
        reactivated: 0,
        errors: 0,
        failedProcessing: 0,
        inflowCount: 0,
        outflowCount: 0,
        highConfidenceCount: 0,
        averageConfidence: 0,
        results: []
      };
      
      let totalConfidence = 0;
      let confidenceCount = 0;
      
      for (const fileData of activeFiles) {
        try {
          // Check if this file was reactivated and needs special handling
          const isReactivated = isFileReactivated(fileData);
          
          let aiResult;
          if (isReactivated && hasExistingAIData(fileData)) {
            // File was reactivated and already has AI data, just restore it
            aiResult = restoreExistingAIData(fileData);
            results.reactivated++;
            debugLog(`Restored existing AI data for: ${fileData.originalFilename}`);
          } else {
            // File needs fresh AI processing
            aiResult = processFileWithAI(fileData);
          }
          
          if (aiResult.success) {
            // Update buffer sheet with AI data
            updateBufferSheetWithAI(bufferSheet, fileData, aiResult.data);
            
            // Add to final sheet (check for duplicates first)
            if (!isDuplicateInFinalSheet(finalSheet, fileData.fileUrl)) {
              addToFinalSheet(finalSheet, fileData, aiResult.data);
              
              // Update statistics
              if (aiResult.data.transactionType === SYSTEM_CONFIG.STATUS.INFLOW) {
                results.inflowCount++;
              } else if (aiResult.data.transactionType === SYSTEM_CONFIG.STATUS.OUTFLOW) {
                results.outflowCount++;
              }
              
              // Track confidence
              const confidence = parseFloat(aiResult.data.confidence) || 0;
              totalConfidence += confidence;
              confidenceCount++;
              
              if (confidence >= SYSTEM_CONFIG.AI.CONFIDENCE_THRESHOLD) {
                results.highConfidenceCount++;
              }
              
            } else {
              debugLog(`Skipped duplicate in final sheet: ${fileData.originalFilename}`);
              results.skipped++;
            }
            
            results.processed++;
            results.results.push({
              filename: fileData.originalFilename,
              newFilename: aiResult.data.newFilename,
              success: true,
              data: aiResult.data,
              reactivated: isReactivated,
              confidence: aiResult.data.confidence
            });
            
          } else {
            // Mark file as failed in buffer sheet
            markFileAsFailed(bufferSheet, fileData, aiResult.error);
            
            results.errors++;
            results.failedProcessing++;
            results.results.push({
              filename: fileData.originalFilename,
              success: false,
              error: aiResult.error,
              reactivated: isReactivated
            });
          }
          
        } catch (error) {
          errorLog(`Error processing file with AI: ${fileData.originalFilename}`, error);
          
          // Mark file as failed
          try {
            markFileAsFailed(bufferSheet, fileData, error.message);
          } catch (markError) {
            errorLog('Error marking file as failed', markError);
          }
          
          results.errors++;
          results.failedProcessing++;
          results.results.push({
            filename: fileData.originalFilename,
            success: false,
            error: error.message,
            reactivated: false
          });
        }
        
        // Add delay to respect API rate limits
        sleep(SYSTEM_CONFIG.PROCESSING.BATCH_DELAY);
      }
      
      // Calculate average confidence
      if (confidenceCount > 0) {
        results.averageConfidence = totalConfidence / confidenceCount;
      }
      
      const resultMessage = `AI processing completed for ${clientName}: ${results.processed}/${results.totalFiles} files processed successfully`;
      
      infoLog(resultMessage, results);
      
      return {
        success: true,
        message: resultMessage,
        ...results
      };
      
    } catch (error) {
      errorLog(`Error processing documents with AI for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Check if a file was reactivated (has deletion reason history)
   */
  function isFileReactivated(fileData) {
    try {
      // Check if the file has a reason that indicates it was reactivated
      return fileData.reason && 
             (fileData.reason.toLowerCase().includes('reactivated on') ||
              fileData.reason.toLowerCase().includes('deleted:'));
    } catch (error) {
      debugLog('Error checking if file was reactivated', error);
      return false;
    }
  }
  
  /**
   * Check if file has existing AI data (changed filename different from original)
   */
  function hasExistingAIData(fileData) {
    try {
      return fileData.changedFilename && 
             fileData.changedFilename !== fileData.originalFilename &&
             fileData.changedFilename.trim() !== '' &&
             fileData.invoiceNumber &&
             fileData.invoiceNumber.trim() !== '';
    } catch (error) {
      debugLog('Error checking existing AI data', error);
      return false;
    }
  }
  
  /**
   * Restore existing AI data for reactivated files
   */
  function restoreExistingAIData(fileData) {
    try {
      debugLog(`Restoring existing AI data for: ${fileData.originalFilename}`);
      
      // Parse existing filename to extract AI data
      let aiData = {
        date: '',
        vendorName: '',
        invoiceNumber: fileData.invoiceNumber || '',
        amount: '',
        documentType: 'invoice',
        transactionType: SYSTEM_CONFIG.STATUS.INFLOW,
        confidence: 0.8, // Default confidence for restored data
        newFilename: fileData.changedFilename,
        processingDate: getCurrentTimestamp()
      };
      
      // If we have a structured filename, parse it
      if (fileData.changedFilename && fileData.changedFilename !== fileData.originalFilename) {
        const parsedData = parseFilenameForAIData(fileData.changedFilename);
        if (parsedData) {
          aiData = { ...aiData, ...parsedData };
        }
      }
      
      debugLog(`Restored AI data for: ${fileData.originalFilename}`, aiData);
      
      return {
        success: true,
        data: aiData,
        restored: true
      };
    } catch (error) {
      errorLog('Error restoring existing AI data', error);
      return {
        success: false,
        error: error.message,
        restored: false
      };
    }
  }
  
  /**
   * Parse filename to extract AI data
   */
  function parseFilenameForAIData(filename) {
    try {
      // Expected format: Date_Vendor_Invoice_Amount.ext
      const nameWithoutExtension = filename.substring(0, filename.lastIndexOf('.'));
      const parts = nameWithoutExtension.split('_');
      
      if (parts.length >= 4) {
        return {
          date: parts[0],
          vendorName: parts[1].replace(/_/g, ' '),
          invoiceNumber: parts[2],
          amount: parts[3]
        };
      }
      
      return null;
    } catch (error) {
      debugLog('Error parsing filename for AI data', error);
      return null;
    }
  }
  
  /**
   * Check if file already exists in final sheet
   */
  function isDuplicateInFinalSheet(finalSheet, fileUrl) {
    try {
      if (finalSheet.getLastRow() <= 1) {
        return false;
      }
      
      const data = finalSheet.getDataRange().getValues();
      const headers = data[0];
      const fileUrlIndex = getColumnIndex(headers, 'File URL');
      
      if (fileUrlIndex === -1) {
        return false;
      }
      
      for (let i = 1; i < data.length; i++) {
        const cellValue = safeGetCellValue(data[i], fileUrlIndex);
        if (cellValue === fileUrl) {
          return true;
        }
      }
      
      return false;
    } catch (error) {
      debugLog('Error checking for duplicate in final sheet', error);
      return false;
    }
  }
  
  /**
   * Process a single file with Gemini AI with enhanced error handling
   */
  function processFileWithAI(fileData) {
    try {
      debugLog(`Processing file with AI: ${fileData.originalFilename}`);
      
      // Validate file data
      if (!fileData.fileUrl) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, 'File URL is required');
      }
      
      // Get file from Drive
      const fileId = extractFileIdFromUrl(fileData.fileUrl);
      if (!fileId) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Invalid file URL: ${fileData.fileUrl}`);
      }
      
      let file;
      try {
        file = DriveApp.getFileById(fileId);
      } catch (error) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.FILE_NOT_FOUND, `File not found in Drive: ${fileData.originalFilename}`);
      }
      
      // Check file size
      const fileSize = file.getSize();
      if (fileSize > SYSTEM_CONFIG.AI.MAX_FILE_SIZE_FOR_AI) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.PROCESSING_FAILED, `File too large for AI processing: ${fileSize} bytes`);
      }
      
      // Check if file type is supported for AI analysis
      const mimeType = file.getBlob().getContentType();
      if (!isAISupportedMimeType(mimeType)) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.PROCESSING_FAILED, `Unsupported file type for AI analysis: ${mimeType}`);
      }
      
      // Process with Gemini AI with retry logic
      const aiResponse = retryWithBackoff(
        () => callGeminiAPI(file, mimeType),
        SYSTEM_CONFIG.AI.MAX_RETRIES,
        SYSTEM_CONFIG.PROCESSING.RETRY_DELAY,
        `AI processing for ${fileData.originalFilename}`
      );
      
      if (aiResponse && aiResponse.data) {
        // Validate AI response
        const validatedData = validateAndCleanAIResponse(aiResponse.data);
        
        // Generate new filename based on AI data
        const newFilename = generateFilenameFromAIData(validatedData, fileData.originalFilename);
        
        // Rename file in Drive
        try {
          if (newFilename !== fileData.originalFilename) {
            file.setName(newFilename);
            debugLog(`Renamed file in Drive: ${fileData.originalFilename} -> ${newFilename}`);
          }
        } catch (renameError) {
          warnLog(`Could not rename file in Drive: ${renameError.message}`);
        }
        
        return {
          success: true,
          data: {
            ...validatedData,
            newFilename: newFilename,
            confidence: aiResponse.confidence || 0.8,
            processingDate: getCurrentTimestamp()
          }
        };
      } else {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.PROCESSING_FAILED, 'Invalid AI response structure');
      }
      
    } catch (error) {
      errorLog(`Error processing file with AI: ${fileData.originalFilename}`, error);
      return {
        success: false,
        error: error.message || 'Unknown AI processing error'
      };
    }
  }
  
  /**
   * Call Gemini AI API with enhanced error handling
   */
  function callGeminiAPI(file, mimeType) {
    try {
      const apiKey = getGeminiApiKey();
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${SYSTEM_CONFIG.AI.MODEL}:generateContent?key=${apiKey}`;
      
      let payload;
      
      if (mimeType.startsWith('image/')) {
        // For images, send both image and text prompt
        const imageBytes = file.getBlob().getBytes();
        const base64Image = Utilities.base64Encode(imageBytes);
        
        payload = {
          contents: [{
            parts: [
              { text: SYSTEM_CONFIG.AI.PROMPTS.DOCUMENT_ANALYSIS },
              {
                inline_data: {
                  mime_type: mimeType,
                  data: base64Image
                }
              }
            ]
          }],
          generationConfig: {
            temperature: 0.1,
            topK: 1,
            topP: 1,
            maxOutputTokens: 2048,
          }
        };
      } else {
        // For text content and PDFs
        let content;
        if (mimeType === 'application/pdf') {
          content = extractPDFMetadata(file);
        } else {
          try {
            content = file.getBlob().getDataAsString();
          } catch (error) {
            content = `File: ${file.getName()} (${mimeType}) - Content extraction failed: ${error.message}`;
          }
        }
        
        payload = {
          contents: [{
            parts: [{
              text: SYSTEM_CONFIG.AI.PROMPTS.DOCUMENT_ANALYSIS + "\n\nDocument content:\n" + content
            }]
          }],
          generationConfig: {
            temperature: 0.1,
            topK: 1,
            topP: 1,
            maxOutputTokens: 2048,
          }
        };
      }
      
      const options = {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        payload: JSON.stringify(payload)
      };
      
      debugLog(`Making Gemini API call for file: ${file.getName()}`);
      
      const response = UrlFetchApp.fetch(url, options);
      const responseData = JSON.parse(response.getContentText());
      
      if (response.getResponseCode() !== 200) {
        const errorMsg = responseData.error?.message || `HTTP ${response.getResponseCode()}`;
        throw createError(SYSTEM_CONFIG.ERROR_CODES.API_LIMIT_EXCEEDED, `Gemini API error: ${errorMsg}`);
      }
      
      if (responseData.candidates && responseData.candidates[0] && responseData.candidates[0].content) {
        const aiText = responseData.candidates[0].content.parts[0].text;
        
        // Try to parse JSON response
        let parsedData;
        try {
          // Clean the response to extract JSON
          const jsonMatch = aiText.match(/\{[\s\S]*\}/);
          if (jsonMatch) {
            parsedData = JSON.parse(jsonMatch[0]);
          } else {
            throw new Error('No JSON found in AI response');
          }
        } catch (parseError) {
          errorLog('Error parsing AI JSON response', parseError);
          debugLog('Raw AI response:', aiText);
          throw createError(SYSTEM_CONFIG.ERROR_CODES.PROCESSING_FAILED, `Failed to parse AI response: ${parseError.message}`);
        }
        
        return {
          data: parsedData,
          confidence: parsedData.confidence || 0.8,
          rawResponse: aiText
        };
      } else {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.PROCESSING_FAILED, 'Invalid response structure from Gemini API');
      }
      
    } catch (error) {
      errorLog('Error calling Gemini API', error);
      throw error;
    }
  }
  
  /**
   * Extract PDF metadata for AI processing
   */
  function extractPDFMetadata(file) {
    try {
      const fileName = file.getName();
      const fileSize = file.getSize();
      const dateCreated = file.getDateCreated();
      
      return `PDF Document Analysis Request:
  Filename: ${fileName}
  File size: ${fileSize} bytes
  Date created: ${dateCreated.toISOString()}
  MIME type: application/pdf
  
  This is a PDF document that needs financial information extraction.
  Please analyze this document and extract:
  - Transaction date
  - Vendor/company name
  - Invoice/document number
  - Amount
  - Document type (invoice, receipt, bill, etc.)
  - Transaction type (inflow for income, outflow for expenses)
  
  Focus on finding invoice details, amounts, dates, and vendor information from this PDF document.`;
    } catch (error) {
      errorLog('Error extracting PDF metadata', error);
      return `PDF Document: ${file.getName()} (metadata extraction failed - ${error.message})`;
    }
  }
  
  /**
   * Check if MIME type is supported for AI analysis
   */
  function isAISupportedMimeType(mimeType) {
    if (!mimeType) return false;
    
    const supportedTypes = [
      'application/pdf',
      'image/jpeg',
      'image/jpg', 
      'image/png',
      'image/gif',
      'image/webp',
      'image/bmp',
      'image/tiff',
      'text/plain',
      'text/csv'
    ];
    
    return supportedTypes.some(type => mimeType.toLowerCase().includes(type));
  }
  
  /**
   * Validate and clean AI response data with comprehensive validation
   */
  function validateAndCleanAIResponse(data) {
    try {
      if (!data || typeof data !== 'object') {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, 'AI response data is not a valid object');
      }
      
      const validated = {
        date: cleanAndValidateDate(data.date),
        vendorName: cleanAndValidateText(data.vendorName, 'Unknown_Vendor'),
        invoiceNumber: cleanAndValidateText(data.invoiceNumber, generateUniqueId().substring(0, 8)),
        amount: cleanAndValidateAmount(data.amount),
        documentType: cleanAndValidateDocumentType(data.documentType),
        transactionType: cleanAndValidateTransactionType(data.transactionType),
        confidence: cleanAndValidateConfidence(data.confidence)
      };
      
      // Validate required fields
      if (!validated.vendorName || validated.vendorName === 'Unknown_Vendor') {
        validated.vendorName = 'Unknown_Vendor';
      }
      
      if (!validated.amount || validated.amount === '0') {
        validated.amount = '0.00';
      }
      
      debugLog('Validated AI response data', validated);
      return validated;
      
    } catch (error) {
      errorLog('Error validating AI response', error);
      
      // Return safe defaults
      return {
        date: formatDateForFilename(new Date()),
        vendorName: 'Unknown_Vendor',
        invoiceNumber: generateUniqueId().substring(0, 8),
        amount: '0.00',
        documentType: 'unknown',
        transactionType: SYSTEM_CONFIG.STATUS.INFLOW,
        confidence: 0.5
      };
    }
  }
  
  /**
   * Clean and validate date format
   */
  function cleanAndValidateDate(dateInput) {
    try {
      if (!dateInput) return formatDateForFilename(new Date());
      
      // If already in YYYY-MM-DD format and valid
      if (typeof dateInput === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateInput)) {
        const testDate = new Date(dateInput);
        if (!isNaN(testDate.getTime())) {
          return dateInput;
        }
      }
      
      // Try to parse as date
      const date = new Date(dateInput);
      if (!isNaN(date.getTime())) {
        return formatDateForFilename(date);
      }
      
      // Fallback to current date
      return formatDateForFilename(new Date());
      
    } catch (error) {
      debugLog(`Error cleaning date: ${dateInput}`, error);
      return formatDateForFilename(new Date());
    }
  }
  
  /**
   * Clean and validate text fields
   */
  function cleanAndValidateText(textInput, defaultValue = '') {
    try {
      if (!textInput || typeof textInput !== 'string') {
        return defaultValue;
      }
      
      // Clean text for filename usage
      const cleaned = textInput
        .trim()
        .replace(/[<>:"/\\|?*\x00-\x1f]/g, '_') // Invalid filename chars
        .replace(/[^\x20-\x7E]/g, '_') // Non-ASCII chars
        .replace(/\s+/g, '_') // Spaces to underscores
        .replace(/_+/g, '_') // Multiple underscores to single
        .replace(/^_|_$/g, ''); // Leading/trailing underscores
      
      return cleaned || defaultValue;
      
    } catch (error) {
      debugLog(`Error cleaning text: ${textInput}`, error);
      return defaultValue;
    }
  }
  
  /**
   * Clean and validate amount
   */
  function cleanAndValidateAmount(amountInput) {
    try {
      if (!amountInput) return '0.00';
      
      // Convert to string and clean
      let cleaned = amountInput.toString()
        .trim()
        .replace(/[$₹€£¥¢₦₵₡₨₩₪₫₽₼₺]/g, '') // Currency symbols
        .replace(/[,\s]/g, '') // Commas and spaces
        .replace(/[()]/g, '') // Parentheses
        .replace(/[^\d.-]/g, ''); // Keep only digits, dots, and minus
      
      // Handle negative amounts in parentheses format
      const isNegative = amountInput.toString().includes('(') && amountInput.toString().includes(')');
      
      // Parse as float
      const parsed = parseFloat(cleaned);
      if (isNaN(parsed)) {
        return '0.00';
      }
      
      const finalAmount = isNegative ? -Math.abs(parsed) : parsed;
      return finalAmount.toFixed(2);
      
    } catch (error) {
      debugLog(`Error cleaning amount: ${amountInput}`, error);
      return '0.00';
    }
  }
  
  /**
   * Clean and validate document type
   */
  function cleanAndValidateDocumentType(typeInput) {
    try {
      if (!typeInput) return 'unknown';
      
      const validTypes = ['invoice', 'receipt', 'bill', 'statement', 'contract', 'other'];
      const cleaned = typeInput.toString().toLowerCase().trim();
      
      return validTypes.includes(cleaned) ? cleaned : 'unknown';
      
    } catch (error) {
      debugLog(`Error cleaning document type: ${typeInput}`, error);
      return 'unknown';
    }
  }
  
  /**
   * Clean and validate transaction type
   */
  function cleanAndValidateTransactionType(typeInput) {
    try {
      if (!typeInput) return SYSTEM_CONFIG.STATUS.INFLOW;
      
      const cleaned = typeInput.toString().toLowerCase().trim();
      
      // Direct match
      if (['inflow', 'outflow'].includes(cleaned)) {
        return cleaned;
      }
      
      // Synonym matching
      const inflowSynonyms = ['income', 'revenue', 'payment_received', 'credit', 'deposit'];
      const outflowSynonyms = ['expense', 'cost', 'payment_made', 'debit', 'withdrawal', 'bill', 'purchase'];
      
      if (inflowSynonyms.some(synonym => cleaned.includes(synonym))) {
        return SYSTEM_CONFIG.STATUS.INFLOW;
      }
      
      if (outflowSynonyms.some(synonym => cleaned.includes(synonym))) {
        return SYSTEM_CONFIG.STATUS.OUTFLOW;
      }
      
      // Default to inflow for safety
      return SYSTEM_CONFIG.STATUS.INFLOW;
      
    } catch (error) {
      debugLog(`Error cleaning transaction type: ${typeInput}`, error);
      return SYSTEM_CONFIG.STATUS.INFLOW;
    }
  }
  
  /**
   * Clean and validate confidence
   */
  function cleanAndValidateConfidence(confidenceInput) {
    try {
      if (!confidenceInput) return 0.8;
      
      const parsed = parseFloat(confidenceInput);
      if (isNaN(parsed)) return 0.8;
      
      // Ensure between 0 and 1
      return Math.max(0, Math.min(1, parsed));
      
    } catch (error) {
      debugLog(`Error cleaning confidence: ${confidenceInput}`, error);
      return 0.8;
    }
  }
  
  /**
   * Generate filename from AI data with proper formatting
   */
  function generateFilenameFromAIData(aiData, originalFilename) {
    try {
      const date = aiData.date || formatDateForFilename(new Date());
      const vendor = cleanFilename(aiData.vendorName) || 'Unknown';
      const invoice = cleanFilename(aiData.invoiceNumber) || 'NoInvoice';
      const amount = aiData.amount || '0.00';
      
      // Get file extension from original filename
      const extension = originalFilename.substring(originalFilename.lastIndexOf('.'));
      
      // Generate new filename: Date_Vendor_Invoice_Amount.extension
      const newFilename = `${date}_${vendor}_${invoice}_${amount}${extension}`;
      
      // Ensure filename length is within limits
      if (newFilename.length > SYSTEM_CONFIG.DRIVE.MAX_FILENAME_LENGTH) {
        const maxBase = SYSTEM_CONFIG.DRIVE.MAX_FILENAME_LENGTH - extension.length - 20; // Leave room for truncation
        const truncated = `${date}_${vendor.substring(0, 20)}_${invoice.substring(0, 15)}_${amount}${extension}`;
        debugLog(`Filename truncated: ${newFilename} -> ${truncated}`);
        return truncated;
      }
      
      debugLog(`Generated filename: ${newFilename} from original: ${originalFilename}`);
      return newFilename;
      
    } catch (error) {
      errorLog('Error generating filename from AI data', error);
      return originalFilename;
    }
  }
  
  /**
   * Get active files from buffer sheet with enhanced filtering
   */
  function getActiveFilesFromBuffer(bufferSheet) {
    try {
      if (bufferSheet.getLastRow() <= 1) {
        return [];
      }
      
      const data = bufferSheet.getDataRange().getValues();
      const headers = data[0];
      const activeFiles = [];
      
      // Find column indices safely
      const originalFilenameIndex = getColumnIndex(headers, 'Original File Name');
      const changedFilenameIndex = getColumnIndex(headers, 'Changed File Name');
      const fileUrlIndex = getColumnIndex(headers, 'File URL');
      const invoiceNumberIndex = getColumnIndex(headers, 'Invoice Number');
      const statusIndex = getColumnIndex(headers, 'Status');
      const reasonIndex = getColumnIndex(headers, 'Reason');
      const emailSubjectIndex = getColumnIndex(headers, 'Email Subject');
      const attemptsIndex = getColumnIndex(headers, 'Processing Attempts');
      
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const status = safeGetCellValue(row, statusIndex);
        const attempts = parseInt(safeGetCellValue(row, attemptsIndex, '0')) || 0;
        
        // Only process active files that haven't exceeded max attempts
        if (status === SYSTEM_CONFIG.STATUS.ACTIVE && attempts < SYSTEM_CONFIG.PROCESSING.MAX_PROCESSING_ATTEMPTS) {
          const fileData = {
            rowIndex: i + 1,
            originalFilename: safeGetCellValue(row, originalFilenameIndex),
            changedFilename: safeGetCellValue(row, changedFilenameIndex),
            fileUrl: safeGetCellValue(row, fileUrlIndex),
            invoiceNumber: safeGetCellValue(row, invoiceNumberIndex),
            emailSubject: safeGetCellValue(row, emailSubjectIndex),
            status: status,
            reason: safeGetCellValue(row, reasonIndex),
            attempts: attempts
          };
          
          // Validate required fields
          if (fileData.originalFilename && fileData.fileUrl) {
            activeFiles.push(fileData);
          } else {
            warnLog(`Skipping invalid file data at row ${i + 1}`, fileData);
          }
        }
      }
      
      debugLog(`Found ${activeFiles.length} active files in buffer sheet`);
      return activeFiles;
      
    } catch (error) {
      errorLog('Error getting active files from buffer sheet', error);
      return [];
    }
  }
  
  /**
   * Update buffer sheet with AI data
   */
  function updateBufferSheetWithAI(bufferSheet, fileData, aiData) {
    try {
      const headers = bufferSheet.getRange(1, 1, 1, bufferSheet.getLastColumn()).getValues()[0];
      
      // Update changed filename
      const changedFilenameIndex = getColumnIndex(headers, 'Changed File Name');
      if (changedFilenameIndex !== -1 && aiData.newFilename) {
        bufferSheet.getRange(fileData.rowIndex, changedFilenameIndex + 1).setValue(aiData.newFilename);
      }
      
      // Update invoice number
      const invoiceNumberIndex = getColumnIndex(headers, 'Invoice Number');
      if (invoiceNumberIndex !== -1 && aiData.invoiceNumber) {
        bufferSheet.getRange(fileData.rowIndex, invoiceNumberIndex + 1).setValue(aiData.invoiceNumber);
      }
      
      // Update last modified
      const lastModifiedIndex = getColumnIndex(headers, 'Last Modified');
      if (lastModifiedIndex !== -1) {
        bufferSheet.getRange(fileData.rowIndex, lastModifiedIndex + 1).setValue(getCurrentTimestamp());
      }
      
      // Update processing attempts
      const attemptsIndex = getColumnIndex(headers, 'Processing Attempts');
      if (attemptsIndex !== -1) {
        const currentAttempts = parseInt(safeGetCellValue([bufferSheet.getRange(fileData.rowIndex, attemptsIndex + 1).getValue()], 0, '0')) || 0;
        bufferSheet.getRange(fileData.rowIndex, attemptsIndex + 1).setValue((currentAttempts + 1).toString());
      }
      
      debugLog(`Updated buffer sheet row ${fileData.rowIndex} with AI data`);
      
    } catch (error) {
      errorLog('Error updating buffer sheet with AI data', error);
      throw error;
    }
  }
  
  /**
   * Mark file as failed in buffer sheet
   */
  function markFileAsFailed(bufferSheet, fileData, errorMessage) {
    try {
      const headers = bufferSheet.getRange(1, 1, 1, bufferSheet.getLastColumn()).getValues()[0];
      
      // Update status to failed
      const statusIndex = getColumnIndex(headers, 'Status');
      if (statusIndex !== -1) {
        bufferSheet.getRange(fileData.rowIndex, statusIndex + 1).setValue(SYSTEM_CONFIG.STATUS.FAILED);
      }
      
      // Update reason with error
      const reasonIndex = getColumnIndex(headers, 'Reason');
      if (reasonIndex !== -1) {
        const failureReason = `AI Processing Failed: ${errorMessage} (${getCurrentTimestamp()})`;
        bufferSheet.getRange(fileData.rowIndex, reasonIndex + 1).setValue(failureReason);
      }
      
      // Update processing attempts
      const attemptsIndex = getColumnIndex(headers, 'Processing Attempts');
      if (attemptsIndex !== -1) {
        const currentAttempts = parseInt(safeGetCellValue([bufferSheet.getRange(fileData.rowIndex, attemptsIndex + 1).getValue()], 0, '0')) || 0;
        bufferSheet.getRange(fileData.rowIndex, attemptsIndex + 1).setValue((currentAttempts + 1).toString());
      }
      
      // Update last modified
      const lastModifiedIndex = getColumnIndex(headers, 'Last Modified');
      if (lastModifiedIndex !== -1) {
        bufferSheet.getRange(fileData.rowIndex, lastModifiedIndex + 1).setValue(getCurrentTimestamp());
      }
      
      debugLog(`Marked file as failed: ${fileData.originalFilename}`);
      
    } catch (error) {
      errorLog('Error marking file as failed', error);
    }
  }
  
  /**
   * Add processed file to final sheet with comprehensive data
   */
  function addToFinalSheet(finalSheet, fileData, aiData) {
    try {
      const uniqueId = generateUniqueId();
      const fileId = extractFileIdFromUrl(fileData.fileUrl);
      
      // Create row data matching final sheet structure
      const rowData = [
        aiData.newFilename || fileData.originalFilename,  // File Name
        uniqueId,                                         // Unique File ID
        fileId || '',                                     // Drive File ID
        fileData.fileUrl,                                // File URL
        '', // Message ID - would need to be passed from Gmail processing
        fileData.emailSubject || '',                     // Email Subject
        '', // Email Sender - would need to be passed from Gmail processing
        aiData.transactionType,                          // Inflow/Outflow Status
        aiData.date,                                     // Date
        aiData.vendorName,                               // Vendor Name
        aiData.invoiceNumber,                            // Invoice Number
        aiData.amount,                                   // Amount
        aiData.documentType,                             // Document Type
        aiData.confidence,                               // AI Confidence
        aiData.processingDate || getCurrentTimestamp(),  // Processing Date
        getCurrentTimestamp()                            // Last Modified
      ];
      
      finalSheet.appendRow(rowData);
      debugLog(`Added file to final sheet: ${aiData.newFilename || fileData.originalFilename}`);
      
    } catch (error) {
      errorLog('Error adding to final sheet', error);
      throw error;
    }
  }
  
  /**
   * Batch process multiple clients with AI
   */
  function processAllClientsWithAI() {
    try {
      infoLog('Starting AI processing for all active clients');
      const activeClients = getActiveClients();
      
      if (activeClients.length === 0) {
        return {
          success: true,
          message: 'No active clients found',
          results: []
        };
      }
      
      const results = [];
      let successCount = 0;
      let failureCount = 0;
      
      for (const client of activeClients) {
        try {
          infoLog(`Processing AI for client: ${client.name}`);
          const result = processClientDocumentsWithAI(client.name);
          results.push({
            client: client.name,
            success: true,
            ...result
          });
          successCount++;
          
          // Add delay between clients to respect rate limits
          sleep(SYSTEM_CONFIG.PROCESSING.RETRY_DELAY * 2);
          
        } catch (error) {
          errorLog(`Error processing AI for client ${client.name}`, error);
          results.push({
            client: client.name,
            success: false,
            error: error.message,
            code: error.code || 'UNKNOWN_ERROR'
          });
          failureCount++;
        }
      }
      
      const summary = {
        success: true,
        message: `Processed ${activeClients.length} clients: ${successCount} successful, ${failureCount} failed`,
        totalClients: activeClients.length,
        successCount: successCount,
        failureCount: failureCount,
        results: results
      };
      
      infoLog('Completed AI processing for all clients', summary);
      return summary;
      
    } catch (error) {
      errorLog('Error in processAllClientsWithAI', error);
      throw error;
    }
  }
  
  /**
   * Get AI processing statistics with enhanced metrics
   */
  function getAIProcessingStats(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const bufferSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME);
      const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
      
      const stats = {
        totalFilesInBuffer: 0,
        totalFilesProcessed: 0,
        pendingProcessing: 0,
        failedProcessing: 0,
        averageConfidence: 0,
        inflowCount: 0,
        outflowCount: 0,
        reactivatedCount: 0,
        highConfidenceCount: 0,
        lastProcessed: null
      };
      
      // Analyze buffer sheet
      if (bufferSheet.getLastRow() > 1) {
        const bufferData = bufferSheet.getDataRange().getValues();
        const headers = bufferData[0];
        
        const statusIndex = getColumnIndex(headers, 'Status');
        const reasonIndex = getColumnIndex(headers, 'Reason');
        const lastModifiedIndex = getColumnIndex(headers, 'Last Modified');
        
        stats.totalFilesInBuffer = bufferData.length - 1;
        
        for (let i = 1; i < bufferData.length; i++) {
          const row = bufferData[i];
          const status = safeGetCellValue(row, statusIndex);
          const reason = safeGetCellValue(row, reasonIndex);
          const lastModified = safeGetCellValue(row, lastModifiedIndex);
          
          if (status === SYSTEM_CONFIG.STATUS.ACTIVE) {
            stats.pendingProcessing++;
          } else if (status === SYSTEM_CONFIG.STATUS.FAILED) {
            stats.failedProcessing++;
          }
          
          if (reason && reason.toLowerCase().includes('reactivated')) {
            stats.reactivatedCount++;
          }
          
          // Track last processed
          if (lastModified && (!stats.lastProcessed || new Date(lastModified) > new Date(stats.lastProcessed))) {
            stats.lastProcessed = lastModified;
          }
        }
      }
      
      // Analyze final sheet
      if (finalSheet.getLastRow() > 1) {
        const finalData = finalSheet.getDataRange().getValues();
        const headers = finalData[0];
        
        stats.totalFilesProcessed = finalData.length - 1;
        
        const confidenceIndex = getColumnIndex(headers, 'AI Confidence');
        const statusIndex = getColumnIndex(headers, 'Inflow/Outflow Status');
        
        let totalConfidence = 0;
        let validConfidenceCount = 0;
        
        for (let i = 1; i < finalData.length; i++) {
          const row = finalData[i];
          
          // Calculate confidence statistics
          const confidence = parseFloat(safeGetCellValue(row, confidenceIndex, '0'));
          if (!isNaN(confidence) && confidence > 0) {
            totalConfidence += confidence;
            validConfidenceCount++;
            
            if (confidence >= SYSTEM_CONFIG.AI.CONFIDENCE_THRESHOLD) {
              stats.highConfidenceCount++;
            }
          }
          
          // Count inflow/outflow
          const status = safeGetCellValue(row, statusIndex, '').toLowerCase();
          if (status === SYSTEM_CONFIG.STATUS.INFLOW) {
            stats.inflowCount++;
          } else if (status === SYSTEM_CONFIG.STATUS.OUTFLOW) {
            stats.outflowCount++;
          }
        }
        
        if (validConfidenceCount > 0) {
          stats.averageConfidence = totalConfidence / validConfidenceCount;
        }
      }
      
      debugLog(`AI processing stats for client: ${clientName}`, stats);
      return stats;
      
    } catch (error) {
      errorLog(`Error getting AI processing stats for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Retry failed AI processing for a client
   */
  function retryFailedAIProcessing(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const bufferSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME);
      
      // Reset failed files to active status
      const data = bufferSheet.getDataRange().getValues();
      if (data.length <= 1) {
        return {
          success: true,
          message: 'No failed files found to retry',
          retriedCount: 0
        };
      }
      
      const headers = data[0];
      const statusIndex = getColumnIndex(headers, 'Status');
      const attemptsIndex = getColumnIndex(headers, 'Processing Attempts');
      const reasonIndex = getColumnIndex(headers, 'Reason');
      
      let retriedCount = 0;
      
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const status = safeGetCellValue(row, statusIndex);
        
        if (status === SYSTEM_CONFIG.STATUS.FAILED) {
          // Reset to active
          bufferSheet.getRange(i + 1, statusIndex + 1).setValue(SYSTEM_CONFIG.STATUS.ACTIVE);
          
          // Reset attempts
          if (attemptsIndex !== -1) {
            bufferSheet.getRange(i + 1, attemptsIndex + 1).setValue('0');
          }
          
          // Update reason
          if (reasonIndex !== -1) {
            bufferSheet.getRange(i + 1, reasonIndex + 1).setValue(`Retry requested on ${getCurrentTimestamp()}`);
          }
          
          retriedCount++;
        }
      }
      
      infoLog(`Reset ${retriedCount} failed files for retry in client: ${clientName}`);
      
      return {
        success: true,
        message: `Reset ${retriedCount} failed files for retry`,
        retriedCount: retriedCount
      };
      
    } catch (error) {
      errorLog(`Error retrying failed AI processing for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Analyze AI processing quality and provide recommendations
   */
  function analyzeAIProcessingQuality(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const stats = getAIProcessingStats(clientName);
      const analysis = {
        overallQuality: 'good',
        confidence: {
          average: stats.averageConfidence,
          highConfidencePercentage: stats.totalFilesProcessed > 0 ? 
            (stats.highConfidenceCount / stats.totalFilesProcessed) * 100 : 0
        },
        processing: {
          successRate: stats.totalFilesInBuffer > 0 ? 
            ((stats.totalFilesInBuffer - stats.failedProcessing) / stats.totalFilesInBuffer) * 100 : 100,
          failureRate: stats.totalFilesInBuffer > 0 ? 
            (stats.failedProcessing / stats.totalFilesInBuffer) * 100 : 0
        },
        distribution: {
          inflowPercentage: stats.totalFilesProcessed > 0 ? 
            (stats.inflowCount / stats.totalFilesProcessed) * 100 : 0,
          outflowPercentage: stats.totalFilesProcessed > 0 ? 
            (stats.outflowCount / stats.totalFilesProcessed) * 100 : 0
        },
        recommendations: []
      };
      
      // Determine overall quality
      if (analysis.confidence.average < 0.6 || analysis.processing.failureRate > 20) {
        analysis.overallQuality = 'poor';
      } else if (analysis.confidence.average < 0.8 || analysis.processing.failureRate > 10) {
        analysis.overallQuality = 'fair';
      } else if (analysis.confidence.average >= 0.9 && analysis.processing.failureRate < 5) {
        analysis.overallQuality = 'excellent';
      }
      
      // Generate recommendations
      if (analysis.confidence.average < 0.7) {
        analysis.recommendations.push('Consider improving document quality or scanning resolution');
      }
      
      if (analysis.processing.failureRate > 15) {
        analysis.recommendations.push('High failure rate detected - check file formats and sizes');
      }
      
      if (analysis.confidence.highConfidencePercentage < 50) {
        analysis.recommendations.push('Many documents have low confidence - manual review recommended');
      }
      
      if (stats.reactivatedCount > stats.totalFilesProcessed * 0.1) {
        analysis.recommendations.push('High reactivation rate - review deletion criteria');
      }
      
      if (analysis.recommendations.length === 0) {
        analysis.recommendations.push('AI processing quality is good - no immediate action needed');
      }
      
      return analysis;
      
    } catch (error) {
      errorLog(`Error analyzing AI processing quality for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Clean up old AI processing data
   */
  function cleanupOldAIData(clientName, daysOld = 30) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
      
      if (finalSheet.getLastRow() <= 1) {
        return { cleaned: 0, message: 'No data to clean' };
      }
      
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysOld);
      
      const data = finalSheet.getDataRange().getValues();
      const headers = data[0];
      const processingDateIndex = getColumnIndex(headers, 'Processing Date');
      
      let cleanedCount = 0;
      
      // Process from bottom to top to avoid index issues
      for (let i = data.length - 1; i >= 1; i--) {
        const row = data[i];
        const processingDate = safeGetCellValue(row, processingDateIndex);
        
        if (processingDate) {
          try {
            const rowDate = new Date(processingDate);
            if (rowDate < cutoffDate) {
              finalSheet.deleteRow(i + 1);
              cleanedCount++;
            }
          } catch (dateError) {
            warnLog(`Invalid processing date in row ${i + 1}: ${processingDate}`);
          }
        }
      }
      
      infoLog(`Cleaned up ${cleanedCount} old AI processing records for client: ${clientName}`);
      return { cleaned: cleanedCount, message: `Cleaned up ${cleanedCount} old records` };
      
    } catch (error) {
      errorLog(`Error cleaning up old AI data for client: ${clientName}`, error);
      throw error;
    }
  }