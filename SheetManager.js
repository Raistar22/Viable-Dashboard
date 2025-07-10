/**
 * SheetManager.gs - Enhanced sheet operations with proper state management (FIXED)
 */

/**
 * Process buffer changes - handles both deletions and reactivations with atomic operations
 */
function processBufferChanges(clientName) {
    let lock;
    try {
      validateInput(clientName, 'string', 'Client name');
      
      infoLog(`Processing buffer changes for client: ${clientName}`);
      
      // Acquire lock for atomic operations
      lock = LockService.getScriptLock();
      if (!lock.tryLock(30000)) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.SYSTEM_ERROR, 'Could not acquire lock for buffer changes processing');
      }
      
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
      
      // Get files marked for deletion and reactivation
      const deletionData = getFilesMarkedForDeletion(bufferSheet);
      const reactivationData = getFilesMarkedForReactivation(bufferSheet);
      
      let deletedCount = 0;
      let reactivatedCount = 0;
      const results = {
        deletions: [],
        reactivations: [],
        errors: []
      };
      
      // Validate deletion requests have reasons
      if (deletionData.length > 0) {
        const missingReasons = deletionData.filter(item => !item.reason || item.reason.trim() === '');
        if (missingReasons.length > 0) {
          throw createError(
            SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT,
            `Deletion reason required for ${missingReasons.length} files. Please provide reasons before processing.`
          );
        }
      }
      
      // Process deletions first
      if (deletionData.length > 0) {
        infoLog(`Processing ${deletionData.length} file deletions`);
        
        const folderStructure = getClientFolderStructure(client);
        const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
        const inflowSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.INFLOW_SHEET_NAME);
        const outflowSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.OUTFLOW_SHEET_NAME);
        
        for (const deletionItem of deletionData) {
          try {
            processFileDeletion(deletionItem, folderStructure, finalSheet, inflowSheet, outflowSheet, bufferSheet);
            deletedCount++;
            results.deletions.push({
              filename: deletionItem.originalFilename,
              success: true,
              reason: deletionItem.reason
            });
          } catch (error) {
            errorLog(`Error processing deletion for file: ${deletionItem.originalFilename}`, error);
            results.errors.push({
              filename: deletionItem.originalFilename,
              operation: 'deletion',
              error: error.message
            });
          }
        }
      }
      
      // Process reactivations
      if (reactivationData.length > 0) {
        infoLog(`Processing ${reactivationData.length} file reactivations`);
        
        for (const reactivationItem of reactivationData) {
          try {
            processFileReactivation(reactivationItem, client, spreadsheet, bufferSheet);
            reactivatedCount++;
            results.reactivations.push({
              filename: reactivationItem.originalFilename,
              success: true
            });
          } catch (error) {
            errorLog(`Error processing reactivation for file: ${reactivationItem.originalFilename}`, error);
            results.errors.push({
              filename: reactivationItem.originalFilename,
              operation: 'reactivation',
              error: error.message
            });
          }
        }
      }
      
      const result = {
        success: true,
        message: `Processed ${deletedCount} deletions and ${reactivatedCount} reactivations`,
        deletedCount: deletedCount,
        reactivatedCount: reactivatedCount,
        errorCount: results.errors.length,
        details: results
      };
      
      infoLog(`Buffer changes processed for client: ${clientName}`, result);
      return result;
      
    } catch (error) {
      errorLog(`Error processing buffer changes for client: ${clientName}`, error);
      throw error;
    } finally {
      if (lock) {
        try {
          lock.releaseLock();
        } catch (releaseError) {
          errorLog('Error releasing lock', releaseError);
        }
      }
    }
  }
  
  /**
   * Get files marked for deletion with enhanced validation
   */
  function getFilesMarkedForDeletion(bufferSheet) {
    try {
      if (bufferSheet.getLastRow() <= 1) {
        return [];
      }
      
      const data = bufferSheet.getDataRange().getValues();
      const headers = data[0];
      const deletionData = [];
      
      // Get column indices safely
      const originalFilenameIndex = getColumnIndex(headers, 'Original File Name');
      const fileUrlIndex = getColumnIndex(headers, 'File URL');
      const fileIdIndex = getColumnIndex(headers, 'File ID');
      const statusIndex = getColumnIndex(headers, 'Status');
      const reasonIndex = getColumnIndex(headers, 'Reason');
      
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const status = safeGetCellValue(row, statusIndex);
        const reason = safeGetCellValue(row, reasonIndex);
        
        // Only include files marked as Deleted that don't already have "Deleted:" prefix
        // This prevents re-processing already deleted files
        if (status === SYSTEM_CONFIG.STATUS.DELETED && !reason.startsWith('Deleted:')) {
          deletionData.push({
            rowIndex: i + 1,
            originalFilename: safeGetCellValue(row, originalFilenameIndex),
            fileUrl: safeGetCellValue(row, fileUrlIndex),
            fileId: safeGetCellValue(row, fileIdIndex),
            reason: reason
          });
        }
      }
      
      debugLog(`Found ${deletionData.length} files marked for deletion`);
      return deletionData;
      
    } catch (error) {
      errorLog('Error getting files marked for deletion', error);
      return [];
    }
  }
  
  /**
   * Get files marked for reactivation with proper detection logic
   */
  function getFilesMarkedForReactivation(bufferSheet) {
    try {
      if (bufferSheet.getLastRow() <= 1) {
        return [];
      }
      
      const data = bufferSheet.getDataRange().getValues();
      const headers = data[0];
      const reactivationData = [];
      
      // Get column indices safely
      const originalFilenameIndex = getColumnIndex(headers, 'Original File Name');
      const changedFilenameIndex = getColumnIndex(headers, 'Changed File Name');
      const fileUrlIndex = getColumnIndex(headers, 'File URL');
      const fileIdIndex = getColumnIndex(headers, 'File ID');
      const invoiceNumberIndex = getColumnIndex(headers, 'Invoice Number');
      const statusIndex = getColumnIndex(headers, 'Status');
      const reasonIndex = getColumnIndex(headers, 'Reason');
      const emailSubjectIndex = getColumnIndex(headers, 'Email Subject');
      const dateAddedIndex = getColumnIndex(headers, 'Date Added');
      
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const status = safeGetCellValue(row, statusIndex);
        const reason = safeGetCellValue(row, reasonIndex);
        
        // Check if file is marked as Active but has a deletion reason (indicates reactivation)
        if (status === SYSTEM_CONFIG.STATUS.ACTIVE && reason.startsWith('Deleted:')) {
          reactivationData.push({
            rowIndex: i + 1,
            originalFilename: safeGetCellValue(row, originalFilenameIndex),
            changedFilename: safeGetCellValue(row, changedFilenameIndex),
            fileUrl: safeGetCellValue(row, fileUrlIndex),
            fileId: safeGetCellValue(row, fileIdIndex),
            invoiceNumber: safeGetCellValue(row, invoiceNumberIndex),
            emailSubject: safeGetCellValue(row, emailSubjectIndex),
            dateAdded: safeGetCellValue(row, dateAddedIndex),
            previousReason: reason
          });
        }
      }
      
      debugLog(`Found ${reactivationData.length} files marked for reactivation`);
      return reactivationData;
      
    } catch (error) {
      errorLog('Error getting files marked for reactivation', error);
      return [];
    }
  }
  
  /**
   * Process file deletion with comprehensive cleanup
   */
  function processFileDeletion(deletionItem, folderStructure, finalSheet, inflowSheet, outflowSheet, bufferSheet) {
    try {
      debugLog(`Processing deletion for file: ${deletionItem.originalFilename}`);
      
      // Step 1: Remove file from Drive folders (but keep in buffer as inactive)
      removeFileFromDriveFolders(deletionItem.fileUrl, folderStructure);
      
      // Step 2: Remove from final sheet
      removeFromSheet(finalSheet, deletionItem.fileUrl, 'File URL');
      
      // Step 3: Remove from inflow/outflow sheets
      removeFromSheet(inflowSheet, deletionItem.fileUrl, 'File URL');
      removeFromSheet(outflowSheet, deletionItem.fileUrl, 'File URL');
      
      // Step 4: Update buffer sheet status with deletion information
      updateBufferSheetForDeletion(bufferSheet, deletionItem.rowIndex, deletionItem.reason);
      
      infoLog(`Successfully processed deletion for file: ${deletionItem.originalFilename}`);
      
    } catch (error) {
      errorLog(`Error processing deletion for file: ${deletionItem.originalFilename}`, error);
      throw error;
    }
  }
  
  /**
   * Process file reactivation with proper state restoration
   */
  function processFileReactivation(reactivationItem, client, spreadsheet, bufferSheet) {
    try {
      debugLog(`Processing reactivation for file: ${reactivationItem.originalFilename}`);
      
      const folderStructure = getClientFolderStructure(client);
      
      // Step 1: Ensure file exists and is accessible in buffer folder
      ensureFileInBufferFolder(reactivationItem, folderStructure.bufferFolder);
      
      // Step 2: Clear the deletion status and update reason
      updateBufferSheetForReactivation(bufferSheet, reactivationItem.rowIndex);
      
      // Step 3: Determine if file needs AI processing
      const needsAIProcessing = !hasValidAIData(reactivationItem);
      
      if (needsAIProcessing) {
        infoLog(`File needs AI processing: ${reactivationItem.originalFilename}`);
        // Mark for AI processing - the AI processor will handle it in the next run
        // We don't process AI immediately to keep operations atomic
      } else {
        // Step 4: File already has AI data, restore to final sheet directly
        restoreToFinalSheet(reactivationItem, spreadsheet);
        infoLog(`Restored file to final sheet: ${reactivationItem.originalFilename}`);
      }
      
      infoLog(`Successfully reactivated file: ${reactivationItem.originalFilename}`);
      
    } catch (error) {
      errorLog(`Error in processFileReactivation for: ${reactivationItem.originalFilename}`, error);
      throw error;
    }
  }
  
  /**
   * Check if reactivation item has valid AI data
   */
  function hasValidAIData(reactivationItem) {
    try {
      return reactivationItem.changedFilename && 
             reactivationItem.changedFilename !== reactivationItem.originalFilename &&
             reactivationItem.invoiceNumber &&
             reactivationItem.invoiceNumber.trim() !== '';
    } catch (error) {
      return false;
    }
  }
  
  /**
   * Ensure file exists in buffer folder
   */
  function ensureFileInBufferFolder(reactivationItem, bufferFolder) {
    try {
      const fileId = extractFileIdFromUrl(reactivationItem.fileUrl);
      if (!fileId) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Invalid file URL: ${reactivationItem.fileUrl}`);
      }
      
      let file;
      try {
        file = DriveApp.getFileById(fileId);
      } catch (error) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.FILE_NOT_FOUND, `File not found in Drive: ${reactivationItem.originalFilename}`);
      }
      
      // Check if file is already in buffer folder
      const parents = file.getParents();
      let isInBuffer = false;
      
      while (parents.hasNext()) {
        const parent = parents.next();
        if (parent.getId() === bufferFolder.getId()) {
          isInBuffer = true;
          break;
        }
      }
      
      if (!isInBuffer) {
        // File was moved out of buffer during deletion, restore it
        bufferFolder.addFile(file);
        
        // Remove from other folders if present (cleanup from previous moves)
        const allParents = file.getParents();
        while (allParents.hasNext()) {
          const parent = allParents.next();
          if (parent.getId() !== bufferFolder.getId()) {
            const parentName = parent.getName();
            if (parentName === SYSTEM_CONFIG.DRIVE.FOLDER_STRUCTURE.INFLOW || 
                parentName === SYSTEM_CONFIG.DRIVE.FOLDER_STRUCTURE.OUTFLOW) {
              parent.removeFile(file);
            }
          }
        }
        
        debugLog(`Restored file to buffer folder: ${reactivationItem.originalFilename}`);
      }
      
    } catch (error) {
      errorLog(`Error ensuring file in buffer: ${reactivationItem.originalFilename}`, error);
      throw error;
    }
  }
  
  /**
   * Update buffer sheet for deletion
   */
  function updateBufferSheetForDeletion(bufferSheet, rowIndex, reason) {
    try {
      const headers = bufferSheet.getRange(1, 1, 1, bufferSheet.getLastColumn()).getValues()[0];
      
      // Update status to Deleted (should already be deleted, but ensure consistency)
      const statusIndex = getColumnIndex(headers, 'Status');
      if (statusIndex !== -1) {
        bufferSheet.getRange(rowIndex, statusIndex + 1).setValue(SYSTEM_CONFIG.STATUS.DELETED);
      }
      
      // Prefix the reason with deletion timestamp
      const reasonIndex = getColumnIndex(headers, 'Reason');
      if (reasonIndex !== -1) {
        const prefixedReason = `Deleted: ${reason} (${getCurrentTimestamp()})`;
        bufferSheet.getRange(rowIndex, reasonIndex + 1).setValue(prefixedReason);
      }
      
      // Update last modified
      const lastModifiedIndex = getColumnIndex(headers, 'Last Modified');
      if (lastModifiedIndex !== -1) {
        bufferSheet.getRange(rowIndex, lastModifiedIndex + 1).setValue(getCurrentTimestamp());
      }
      
      debugLog(`Updated buffer sheet for deletion at row: ${rowIndex}`);
      
    } catch (error) {
      errorLog('Error updating buffer sheet for deletion', error);
      throw error;
    }
  }
  
  /**
   * Update buffer sheet for reactivation
   */
  function updateBufferSheetForReactivation(bufferSheet, rowIndex) {
    try {
      const headers = bufferSheet.getRange(1, 1, 1, bufferSheet.getLastColumn()).getValues()[0];
      
      // Update status to Active
      const statusIndex = getColumnIndex(headers, 'Status');
      if (statusIndex !== -1) {
        bufferSheet.getRange(rowIndex, statusIndex + 1).setValue(SYSTEM_CONFIG.STATUS.ACTIVE);
      }
      
      // Update reason to show reactivation history
      const reasonIndex = getColumnIndex(headers, 'Reason');
      if (reasonIndex !== -1) {
        const currentReason = bufferSheet.getRange(rowIndex, reasonIndex + 1).getValue();
        const newReason = `Reactivated on ${getCurrentTimestamp()}. Previous: ${currentReason}`;
        bufferSheet.getRange(rowIndex, reasonIndex + 1).setValue(newReason);
      }
      
      // Reset processing attempts
      const attemptsIndex = getColumnIndex(headers, 'Processing Attempts');
      if (attemptsIndex !== -1) {
        bufferSheet.getRange(rowIndex, attemptsIndex + 1).setValue('0');
      }
      
      // Update last modified
      const lastModifiedIndex = getColumnIndex(headers, 'Last Modified');
      if (lastModifiedIndex !== -1) {
        bufferSheet.getRange(rowIndex, lastModifiedIndex + 1).setValue(getCurrentTimestamp());
      }
      
      debugLog(`Updated buffer sheet for reactivation at row: ${rowIndex}`);
      
    } catch (error) {
      errorLog('Error updating buffer sheet for reactivation', error);
      throw error;
    }
  }
  
  /**
   * Remove file from Drive folders (not trash, just remove from inflow/outflow)
   */
  function removeFileFromDriveFolders(fileUrl, folderStructure) {
    try {
      const fileId = extractFileIdFromUrl(fileUrl);
      if (!fileId) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Invalid file URL: ${fileUrl}`);
      }
      
      const file = DriveApp.getFileById(fileId);
      
      // Remove from inflow/outflow folders only, keep in buffer
      const foldersToRemoveFrom = [
        folderStructure.inflowFolder,
        folderStructure.outflowFolder
      ];
      
      for (const folder of foldersToRemoveFrom) {
        try {
          // Check if file is in this folder
          const filesInFolder = folder.getFilesByName(file.getName());
          while (filesInFolder.hasNext()) {
            const fileInFolder = filesInFolder.next();
            if (fileInFolder.getId() === file.getId()) {
              folder.removeFile(file);
              debugLog(`Removed file from folder: ${folder.getName()}`);
              break;
            }
          }
        } catch (error) {
          warnLog(`Error removing file from folder ${folder.getName()}`, error);
        }
      }
      
    } catch (error) {
      errorLog(`Error removing file from Drive folders: ${fileUrl}`, error);
      throw error;
    }
  }
  
  /**
   * Generic function to remove from sheet by matching column value
   */
  function removeFromSheet(sheet, valueToMatch, columnHeader) {
    try {
      if (sheet.getLastRow() <= 1) {
        return 0; // No data to remove
      }
      
      const data = sheet.getDataRange().getValues();
      const headers = data[0];
      const columnIndex = getColumnIndex(headers, columnHeader);
      
      if (columnIndex === -1) {
        warnLog(`Column '${columnHeader}' not found in sheet: ${sheet.getName()}`);
        return 0;
      }
      
      let removedCount = 0;
      
      // Process from bottom to top to avoid index issues
      for (let i = data.length - 1; i >= 1; i--) {
        const row = data[i];
        const cellValue = safeGetCellValue(row, columnIndex);
        if (cellValue === valueToMatch) {
          sheet.deleteRow(i + 1);
          removedCount++;
          debugLog(`Deleted row from ${sheet.getName()}: ${i + 1}`);
        }
      }
      
      return removedCount;
      
    } catch (error) {
      errorLog(`Error removing from sheet: ${sheet.getName()}`, error);
      throw error;
    }
  }
  
  /**
   * Restore file to final sheet with existing AI data
   */
  function restoreToFinalSheet(reactivationItem, spreadsheet) {
    try {
      const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
      const uniqueId = generateUniqueId();
      
      // Parse AI data from existing filename and invoice number
      let aiData = {
        date: '',
        vendorName: '',
        invoiceNumber: reactivationItem.invoiceNumber || '',
        amount: '',
        transactionType: SYSTEM_CONFIG.STATUS.INFLOW // default
      };
      
      if (reactivationItem.changedFilename && 
          reactivationItem.changedFilename !== reactivationItem.originalFilename) {
        // Parse data from filename format: Date_Vendor_Invoice_Amount.ext
        const parsedData = parseFilenameForAIData(reactivationItem.changedFilename);
        if (parsedData) {
          aiData = { ...aiData, ...parsedData };
        }
      }
      
      const rowData = [
        reactivationItem.changedFilename || reactivationItem.originalFilename, // File Name
        uniqueId,                                 // Unique File ID
        reactivationItem.fileId || '',           // Drive File ID
        reactivationItem.fileUrl,                // File URL
        '', // Message ID - not available in reactivation data
        reactivationItem.emailSubject || '',     // Email Subject
        '', // Email Sender - not available in reactivation data
        aiData.transactionType,                  // Inflow/Outflow Status
        aiData.date,                             // Date
        aiData.vendorName,                       // Vendor Name
        aiData.invoiceNumber,                    // Invoice Number
        aiData.amount,                           // Amount
        'restored', // Document Type
        0.8,                                     // AI Confidence (default for restored)
        getCurrentTimestamp(),                   // Processing Date
        getCurrentTimestamp()                    // Last Modified
      ];
      
      finalSheet.appendRow(rowData);
      debugLog(`Restored file to final sheet: ${reactivationItem.changedFilename || reactivationItem.originalFilename}`);
      
    } catch (error) {
      errorLog('Error restoring to final sheet', error);
      throw error;
    }
  }
  
  /**
   * Enhanced move files to inflow/outflow with better error handling
   */
  function moveFilesToInflowOutflow(clientName) {
    let lock;
    try {
      validateInput(clientName, 'string', 'Client name');
      
      infoLog(`Moving files to inflow/outflow for client: ${clientName}`);
      
      // Acquire lock for atomic operations
      lock = LockService.getScriptLock();
      if (!lock.tryLock(30000)) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.SYSTEM_ERROR, 'Could not acquire lock for flow processing');
      }
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
      const inflowSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.INFLOW_SHEET_NAME);
      const outflowSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.OUTFLOW_SHEET_NAME);
      
      // Get files from final sheet
      const finalSheetData = getFinalSheetData(finalSheet);
      
      if (finalSheetData.length === 0) {
        return {
          success: true,
          message: 'No files to move from final sheet',
          inflowCount: 0,
          outflowCount: 0,
          errorCount: 0
        };
      }
      
      let inflowCount = 0;
      let outflowCount = 0;
      let errorCount = 0;
      const folderStructure = getClientFolderStructure(client);
      const errors = [];
      
      // Process each file
      for (const fileData of finalSheetData) {
        try {
          const isInflow = fileData.inflowOutflowStatus.toLowerCase() === SYSTEM_CONFIG.STATUS.INFLOW;
          
          if (isInflow) {
            // Add to inflow sheet and move to inflow folder
            addToInflowSheet(inflowSheet, fileData);
            moveFileToFolder(fileData.fileUrl, folderStructure.bufferFolder, folderStructure.inflowFolder);
            inflowCount++;
          } else {
            // Add to outflow sheet and move to outflow folder
            addToOutflowSheet(outflowSheet, fileData);
            moveFileToFolder(fileData.fileUrl, folderStructure.bufferFolder, folderStructure.outflowFolder);
            outflowCount++;
          }
          
          debugLog(`Moved file to ${isInflow ? 'inflow' : 'outflow'}: ${fileData.fileName}`);
          
        } catch (error) {
          errorLog(`Error moving file: ${fileData.fileName}`, error);
          errorCount++;
          errors.push({
            fileName: fileData.fileName,
            error: error.message
          });
        }
      }
      
      // Clear final sheet after successful moves (only if no errors)
      if (errorCount === 0) {
        clearFinalSheet(finalSheet);
      } else {
        warnLog(`Not clearing final sheet due to ${errorCount} errors`);
      }
      
      const result = {
        success: true,
        message: `Successfully moved ${inflowCount} files to inflow and ${outflowCount} files to outflow${errorCount > 0 ? ` (${errorCount} errors)` : ''}`,
        inflowCount: inflowCount,
        outflowCount: outflowCount,
        errorCount: errorCount,
        errors: errors
      };
      
      infoLog(`Files moved to inflow/outflow for client: ${clientName}`, result);
      return result;
      
    } catch (error) {
      errorLog(`Error moving files to inflow/outflow for client: ${clientName}`, error);
      throw error;
    } finally {
      if (lock) {
        try {
          lock.releaseLock();
        } catch (releaseError) {
          errorLog('Error releasing lock', releaseError);
        }
      }
    }
  }
  
  /**
   * Enhanced file movement between folders
   */
  function moveFileToFolder(fileUrl, sourceFolder, targetFolder) {
    try {
      const fileId = extractFileIdFromUrl(fileUrl);
      if (!fileId) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Invalid file URL: ${fileUrl}`);
      }
      
      const file = DriveApp.getFileById(fileId);
      
      // Add to target folder first
      targetFolder.addFile(file);
      
      // Remove from source folder
      sourceFolder.removeFile(file);
      
      debugLog(`Moved file from ${sourceFolder.getName()} to ${targetFolder.getName()}: ${file.getName()}`);
      
    } catch (error) {
      errorLog(`Error moving file between folders: ${fileUrl}`, error);
      throw error;
    }
  }
  
  /**
   * Get final sheet data with enhanced validation
   */
  function getFinalSheetData(finalSheet) {
    try {
      if (finalSheet.getLastRow() <= 1) {
        return [];
      }
      
      const data = finalSheet.getDataRange().getValues();
      const headers = data[0];
      const finalSheetData = [];
      
      // Get column indices safely
      const fileNameIndex = getColumnIndex(headers, 'File Name');
      const uniqueFileIdIndex = getColumnIndex(headers, 'Unique File ID');
      const driveFileIdIndex = getColumnIndex(headers, 'Drive File ID');
      const fileUrlIndex = getColumnIndex(headers, 'File URL');
      const messageIdIndex = getColumnIndex(headers, 'Message ID');
      const emailSubjectIndex = getColumnIndex(headers, 'Email Subject');
      const emailSenderIndex = getColumnIndex(headers, 'Email Sender');
      const inflowOutflowStatusIndex = getColumnIndex(headers, 'Inflow/Outflow Status');
      const dateIndex = getColumnIndex(headers, 'Date');
      const vendorNameIndex = getColumnIndex(headers, 'Vendor Name');
      const invoiceNumberIndex = getColumnIndex(headers, 'Invoice Number');
      const amountIndex = getColumnIndex(headers, 'Amount');
      const documentTypeIndex = getColumnIndex(headers, 'Document Type');
      const aiConfidenceIndex = getColumnIndex(headers, 'AI Confidence');
      const processingDateIndex = getColumnIndex(headers, 'Processing Date');
      
      for (let i = 1; i < data.length; i++) {
        const row = data[i];
        
        // Validate required fields
        const fileName = safeGetCellValue(row, fileNameIndex);
        const fileUrl = safeGetCellValue(row, fileUrlIndex);
        const inflowOutflowStatus = safeGetCellValue(row, inflowOutflowStatusIndex);
        
        if (fileName && fileUrl && inflowOutflowStatus) {
          finalSheetData.push({
            fileName: fileName,
            uniqueFileId: safeGetCellValue(row, uniqueFileIdIndex),
            driveFileId: safeGetCellValue(row, driveFileIdIndex),
            fileUrl: fileUrl,
            messageId: safeGetCellValue(row, messageIdIndex),
            emailSubject: safeGetCellValue(row, emailSubjectIndex),
            emailSender: safeGetCellValue(row, emailSenderIndex),
            inflowOutflowStatus: inflowOutflowStatus,
            date: safeGetCellValue(row, dateIndex),
            vendorName: safeGetCellValue(row, vendorNameIndex),
            invoiceNumber: safeGetCellValue(row, invoiceNumberIndex),
            amount: safeGetCellValue(row, amountIndex),
            documentType: safeGetCellValue(row, documentTypeIndex),
            aiConfidence: safeGetCellValue(row, aiConfidenceIndex),
            processingDate: safeGetCellValue(row, processingDateIndex)
          });
        } else {
          warnLog(`Skipping invalid row in final sheet: ${i + 1}`, {
            fileName, fileUrl, inflowOutflowStatus
          });
        }
      }
      
      debugLog(`Retrieved ${finalSheetData.length} valid files from final sheet`);
      return finalSheetData;
      
    } catch (error) {
      errorLog('Error getting final sheet data', error);
      return [];
    }
  }
  
  /**
   * Add to inflow sheet with validation
   */
  function addToInflowSheet(inflowSheet, fileData) {
    try {
      const rowData = [
        fileData.fileName,
        fileData.uniqueFileId,
        fileData.driveFileId,
        fileData.fileUrl,
        fileData.messageId,
        fileData.emailSubject,
        fileData.emailSender,
        fileData.date,
        fileData.vendorName,
        fileData.invoiceNumber,
        fileData.amount,
        fileData.documentType,
        fileData.aiConfidence,
        fileData.processingDate,
        getCurrentTimestamp() // Moved Date
      ];
      
      inflowSheet.appendRow(rowData);
      debugLog(`Added file to inflow sheet: ${fileData.fileName}`);
      
    } catch (error) {
      errorLog('Error adding to inflow sheet', error);
      throw error;
    }
  }
  
  /**
   * Add to outflow sheet with validation
   */
  function addToOutflowSheet(outflowSheet, fileData) {
    try {
      const rowData = [
        fileData.fileName,
        fileData.uniqueFileId,
        fileData.driveFileId,
        fileData.fileUrl,
        fileData.messageId,
        fileData.emailSubject,
        fileData.emailSender,
        fileData.date,
        fileData.vendorName,
        fileData.invoiceNumber,
        fileData.amount,
        fileData.documentType,
        fileData.aiConfidence,
        fileData.processingDate,
        getCurrentTimestamp() // Moved Date
      ];
      
      outflowSheet.appendRow(rowData);
      debugLog(`Added file to outflow sheet: ${fileData.fileName}`);
      
    } catch (error) {
      errorLog('Error adding to outflow sheet', error);
      throw error;
    }
  }
  
  /**
   * Clear final sheet after processing
   */
  function clearFinalSheet(finalSheet) {
    try {
      const lastRow = finalSheet.getLastRow();
      if (lastRow > 1) {
        finalSheet.deleteRows(2, lastRow - 1);
        debugLog('Cleared final sheet');
      }
    } catch (error) {
      errorLog('Error clearing final sheet', error);
      throw error;
    }
  }
  
  /**
   * Get comprehensive sheet statistics
   */
  function getSheetStatistics(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const bufferSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME);
      const finalSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME);
      const inflowSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.INFLOW_SHEET_NAME);
      const outflowSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.OUTFLOW_SHEET_NAME);
      
      const stats = {
        buffer: {
          total: Math.max(0, bufferSheet.getLastRow() - 1),
          active: 0,
          deleted: 0,
          failed: 0,
          processing: 0
        },
        final: Math.max(0, finalSheet.getLastRow() - 1),
        inflow: Math.max(0, inflowSheet.getLastRow() - 1),
        outflow: Math.max(0, outflowSheet.getLastRow() - 1),
        lastUpdate: getCurrentTimestamp()
      };
      
      // Analyze buffer sheet in detail
      if (bufferSheet.getLastRow() > 1) {
        const bufferData = bufferSheet.getDataRange().getValues();
        const headers = bufferData[0];
        const statusIndex = getColumnIndex(headers, 'Status');
        
        for (let i = 1; i < bufferData.length; i++) {
          const status = safeGetCellValue(bufferData[i], statusIndex);
          
          switch (status) {
            case SYSTEM_CONFIG.STATUS.ACTIVE:
              stats.buffer.active++;
              break;
            case SYSTEM_CONFIG.STATUS.DELETED:
              stats.buffer.deleted++;
              break;
            case SYSTEM_CONFIG.STATUS.FAILED:
              stats.buffer.failed++;
              break;
            case SYSTEM_CONFIG.STATUS.PROCESSING:
              stats.buffer.processing++;
              break;
          }
        }
      }
      
      return stats;
      
    } catch (error) {
      errorLog(`Error getting sheet statistics for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Validate and repair sheet structure
   */
  function validateAndRepairSheetStructure(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const validation = {
        isValid: true,
        errors: [],
        warnings: [],
        repairs: []
      };
      
      // Check and repair each required sheet
      const requiredSheets = [
        SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME,
        SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME,
        SYSTEM_CONFIG.SHEETS.INFLOW_SHEET_NAME,
        SYSTEM_CONFIG.SHEETS.OUTFLOW_SHEET_NAME
      ];
      
      for (const sheetName of requiredSheets) {
        try {
          let sheet = spreadsheet.getSheetByName(sheetName);
          
          if (!sheet) {
            // Create missing sheet
            sheet = spreadsheet.insertSheet(sheetName);
            setupSheetStructure(sheet, sheetName);
            validation.repairs.push(`Created missing sheet: ${sheetName}`);
          } else {
            // Validate existing sheet
            const sheetValidation = validateSheetHeaders(sheet, sheetName);
            if (!sheetValidation.isValid) {
              validation.warnings.push(`Sheet '${sheetName}' has header issues`);
            }
          }
        } catch (error) {
          validation.errors.push(`Error with sheet '${sheetName}': ${error.message}`);
          validation.isValid = false;
        }
      }
      
      return validation;
      
    } catch (error) {
      errorLog(`Error validating sheet structure for client: ${clientName}`, error);
      return {
        isValid: false,
        errors: [`Validation failed: ${error.message}`],
        warnings: [],
        repairs: []
      };
    }
  }
  
  /**
   * Export sheet data to CSV
   */
  function exportSheetToCSV(clientName, sheetName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      validateInput(sheetName, 'string', 'Sheet name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const sheet = spreadsheet.getSheetByName(sheetName);
      
      if (!sheet) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Sheet '${sheetName}' not found`);
      }
      
      if (sheet.getLastRow() === 0) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Sheet '${sheetName}' is empty`);
      }
      
      const data = sheet.getDataRange().getValues();
      const csvContent = data.map(row => 
        row.map(cell => {
          // Escape quotes and wrap in quotes if contains comma, quote, or newline
          const cellStr = String(cell || '');
          if (cellStr.includes(',') || cellStr.includes('"') || cellStr.includes('\n')) {
            return `"${cellStr.replace(/"/g, '""')}"`;
          }
          return cellStr;
        }).join(',')
      ).join('\n');
      
      // Create CSV file in client's spreadsheets folder
      const folderStructure = getClientFolderStructure(client);
      const csvFileName = `${cleanFilename(clientName)}_${cleanFilename(sheetName)}_Export_${formatDateForFilename(new Date())}.csv`;
      const csvBlob = Utilities.newBlob(csvContent, 'text/csv', csvFileName);
      const csvFile = folderStructure.spreadsheetsFolder.createFile(csvBlob);
      
      return {
        success: true,
        fileName: csvFileName,
        fileUrl: csvFile.getUrl(),
        fileId: csvFile.getId(),
        recordCount: data.length - 1 // Exclude header
      };
      
    } catch (error) {
      errorLog(`Error exporting sheet to CSV for client: ${clientName}, sheet: ${sheetName}`, error);
      throw error;
    }
  }
  
  /**
   * Archive old data from sheets
   */
  function archiveOldData(clientName, daysOld = 30) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const archiveDate = new Date();
      archiveDate.setDate(archiveDate.getDate() - daysOld);
      
      const archived = {
        buffer: 0,
        inflow: 0,
        outflow: 0
      };
      
      // Archive from each sheet
      const sheetsToArchive = [
        { name: SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME, key: 'buffer', dateColumn: 'Date Added' },
        { name: SYSTEM_CONFIG.SHEETS.INFLOW_SHEET_NAME, key: 'inflow', dateColumn: 'Moved Date' },
        { name: SYSTEM_CONFIG.SHEETS.OUTFLOW_SHEET_NAME, key: 'outflow', dateColumn: 'Moved Date' }
      ];
      
      for (const sheetInfo of sheetsToArchive) {
        try {
          const sheet = spreadsheet.getSheetByName(sheetInfo.name);
          if (sheet && sheet.getLastRow() > 1) {
            const archived_count = archiveSheetData(spreadsheet, sheet, sheetInfo.dateColumn, archiveDate);
            archived[sheetInfo.key] = archived_count;
          }
        } catch (error) {
          warnLog(`Error archiving data from sheet: ${sheetInfo.name}`, error);
        }
      }
      
      const totalArchived = archived.buffer + archived.inflow + archived.outflow;
      
      return {
        success: true,
        archived: archived,
        totalArchived: totalArchived,
        message: `Archived ${totalArchived} records older than ${daysOld} days`
      };
      
    } catch (error) {
      errorLog(`Error archiving old data for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Archive data from a specific sheet
   */
  function archiveSheetData(spreadsheet, sheet, dateColumnName, archiveDate) {
    try {
      const data = sheet.getDataRange().getValues();
      const headers = data[0];
      const dateColumnIndex = getColumnIndex(headers, dateColumnName);
      
      if (dateColumnIndex === -1) {
        warnLog(`Date column '${dateColumnName}' not found in sheet: ${sheet.getName()}`);
        return 0;
      }
      
      const rowsToArchive = [];
      const rowsToDelete = [];
      
      // Collect rows to archive (from bottom to top to maintain indices)
      for (let i = data.length - 1; i >= 1; i--) {
        const row = data[i];
        const rowDate = safeGetCellValue(row, dateColumnIndex);
        
        if (rowDate) {
          try {
            const date = new Date(rowDate);
            if (date < archiveDate) {
              rowsToArchive.unshift(row); // Add to beginning to maintain order
              rowsToDelete.push(i + 1); // Store 1-based row index
            }
          } catch (dateError) {
            warnLog(`Invalid date in row ${i + 1}: ${rowDate}`);
          }
        }
      }
      
      if (rowsToArchive.length === 0) {
        return 0;
      }
      
      // Create archive sheet
      const archiveSheetName = `${sheet.getName()}_Archive_${formatDateForFilename(new Date())}`;
      const archiveSheet = spreadsheet.insertSheet(archiveSheetName);
      
      // Set up archive sheet with headers
      archiveSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      
      // Add archived data
      if (rowsToArchive.length > 0) {
        archiveSheet.getRange(2, 1, rowsToArchive.length, headers.length).setValues(rowsToArchive);
      }
      
      // Format archive sheet
      const headerRange = archiveSheet.getRange(1, 1, 1, headers.length);
      headerRange.setFontWeight('bold');
      headerRange.setBackground('#cccccc');
      
      // Delete archived rows from original sheet
      for (const rowIndex of rowsToDelete) {
        sheet.deleteRow(rowIndex);
      }
      
      infoLog(`Archived ${rowsToArchive.length} rows from ${sheet.getName()}`);
      return rowsToArchive.length;
      
    } catch (error) {
      errorLog(`Error archiving data from sheet: ${sheet.getName()}`, error);
      return 0;
    }
  }
  
  /**
   * Clean up empty rows and optimize sheets
   */
  function cleanupAndOptimizeSheets(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        throw createError(SYSTEM_CONFIG.ERROR_CODES.INVALID_INPUT, `Client '${clientName}' not found`);
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      let totalCleaned = 0;
      const results = {};
      
      const sheetsToClean = [
        SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME,
        SYSTEM_CONFIG.SHEETS.FINAL_SHEET_NAME, 
        SYSTEM_CONFIG.SHEETS.INFLOW_SHEET_NAME, 
        SYSTEM_CONFIG.SHEETS.OUTFLOW_SHEET_NAME
      ];
      
      for (const sheetName of sheetsToClean) {
        try {
          const sheet = spreadsheet.getSheetByName(sheetName);
          if (sheet && sheet.getLastRow() > 1) {
            const cleaned = cleanupEmptyRowsInSheet(sheet);
            results[sheetName] = cleaned;
            totalCleaned += cleaned;
          }
        } catch (error) {
          warnLog(`Error cleaning sheet: ${sheetName}`, error);
          results[sheetName] = 0;
        }
      }
      
      return {
        success: true,
        totalCleaned: totalCleaned,
        results: results,
        message: `Cleaned up ${totalCleaned} empty rows across all sheets`
      };
      
    } catch (error) {
      errorLog(`Error cleaning up sheets for client: ${clientName}`, error);
      throw error;
    }
  }
  
  /**
   * Clean up empty rows in a specific sheet
   */
  function cleanupEmptyRowsInSheet(sheet) {
    try {
      let cleanedCount = 0;
      
      if (sheet.getLastRow() <= 1) {
        return cleanedCount;
      }
      
      const data = sheet.getDataRange().getValues();
      
      // Process from bottom to top to avoid index issues
      for (let i = data.length - 1; i >= 1; i--) {
        const row = data[i];
        const isEmpty = row.every(cell => !cell || cell.toString().trim() === '');
        
        if (isEmpty) {
          sheet.deleteRow(i + 1);
          cleanedCount++;
        }
      }
      
      return cleanedCount;
      
    } catch (error) {
      errorLog(`Error cleaning empty rows in sheet: ${sheet.getName()}`, error);
      return 0;
    }
  }
  
  /**
   * Get pending buffer changes summary
   */
  function getPendingBufferChanges(clientName) {
    try {
      validateInput(clientName, 'string', 'Client name');
      
      const client = getClientByName(clientName);
      if (!client) {
        return {
          deletions: [],
          reactivations: [],
          hasChanges: false,
          error: `Client '${clientName}' not found`
        };
      }
      
      const spreadsheet = SpreadsheetApp.openById(client.spreadsheetId);
      const bufferSheet = getOrCreateSheet(spreadsheet, SYSTEM_CONFIG.SHEETS.BUFFER_SHEET_NAME);
      
      const deletions = getFilesMarkedForDeletion(bufferSheet);
      const reactivations = getFilesMarkedForReactivation(bufferSheet);
      
      return {
        deletions: deletions,
        reactivations: reactivations,
        hasChanges: deletions.length > 0 || reactivations.length > 0,
        summary: {
          deletionCount: deletions.length,
          reactivationCount: reactivations.length,
          totalChanges: deletions.length + reactivations.length
        }
      };
      
    } catch (error) {
      errorLog(`Error getting pending buffer changes for client: ${clientName}`, error);
      return {
        deletions: [],
        reactivations: [],
        hasChanges: false,
        error: error.message
      };
    }
  }