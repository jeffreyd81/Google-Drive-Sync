import google_drive from "@pipedream/google_drive";

export default defineComponent({
  name: "Copy Google Drive Folders and Files",
  description: "Execute bulk copying of folders and files from one Google Drive location to another, creating folder structure and transferring files",
  type: "action",
  props: {
    google_drive,
    sourceFolderId: {
      propDefinition: [google_drive, "folderId"],
      label: "Source Folder",
      description: "The source folder to copy from (leave empty to copy from root)",
      optional: true,
    },
    sourceFileIds: {
      type: "string[]",
      label: "Source Files (Optional)",
      description: "Specific files to copy. If not provided, all files in the source folder will be copied",
      optional: true,
      async options({ sourceFolderId, prevContext }) {
        const { nextPageToken } = prevContext;
        if (!sourceFolderId) return [];
        
        const baseOpts = {
          q: `'${sourceFolderId}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false`,
        };
        
        return this.google_drive.listDriveFilesOptions(null, nextPageToken, baseOpts);
      },
    },
    destinationFolderId: {
      propDefinition: [google_drive, "folderId"],
      label: "Destination Folder",
      description: "The destination folder to copy to",
    },
    includeSubfolders: {
      type: "boolean",
      label: "Include Subfolders",
      description: "Whether to recursively copy subfolders and their contents",
      default: true,
    },
    conflictResolution: {
      type: "string",
      label: "Conflict Resolution",
      description: "How to handle files that already exist in the destination",
      options: [
        { label: "Skip existing files", value: "skip" },
        { label: "Rename duplicates", value: "rename" },
        { label: "Replace existing files", value: "replace" },
      ],
      default: "rename",
    },
    preserveStructure: {
      type: "boolean",
      label: "Preserve Folder Structure",
      description: "Maintain the original folder structure in the destination",
      default: true,
    },
    batchSize: {
      type: "integer",
      label: "Batch Size",
      description: "Number of files to process in each batch (smaller values reduce memory usage)",
      optional: true,
      default: 20,
      min: 1,
      max: 100,
    },
    maxConcurrency: {
      type: "integer",
      label: "Max Concurrent Operations",
      description: "Maximum number of files to copy concurrently",
      optional: true,
      default: 5,
      min: 1,
      max: 20,
    },
  },
  methods: {
    async delay(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    },

    async processInBatches(items, batchSize, processor) {
      const results = [];
      for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);
        const batchResults = await processor(batch, i);
        results.push(...batchResults);
        
        // Small delay between batches to prevent rate limiting
        if (i + batchSize < items.length) {
          await this.delay(100);
        }
      }
      return results;
    },

    async limitConcurrency(tasks, maxConcurrency) {
      const results = [];
      for (let i = 0; i < tasks.length; i += maxConcurrency) {
        const batch = tasks.slice(i, i + maxConcurrency);
        const batchResults = await Promise.allSettled(batch);
        results.push(...batchResults);
      }
      return results;
    },

    async createFolderStructureOptimized(sourceId, destParentId, folderPath = "", folderCache = new Map()) {
      const folders = [];
      
      if (!sourceId) return folders;

      try {
        // Get all subfolders in one API call
        const subfolders = await this.google_drive.listFilesInPage(null, {
          q: `'${sourceId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
          fields: "files(id,name,parents)",
          pageSize: 100,
        });

        // Check existing folders in destination in one API call
        const existingFolders = await this.google_drive.listFilesInPage(null, {
          q: `'${destParentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
          fields: "files(id,name)",
          pageSize: 100,
        });

        const existingFolderMap = new Map(
          existingFolders.files?.map(f => [f.name, f.id]) || []
        );

        // Process folders in batches
        for (const folder of subfolders.files || []) {
          const currentPath = folderPath ? `${folderPath}/${folder.name}` : folder.name;
          let destFolderId;

          if (existingFolderMap.has(folder.name)) {
            destFolderId = existingFolderMap.get(folder.name);
          } else {
            const newFolder = await this.google_drive.createFolder({
              name: folder.name,
              parentId: destParentId,
            });
            destFolderId = newFolder.id;
          }

          folderCache.set(folder.id, destFolderId);
          folders.push({
            sourceId: folder.id,
            destId: destFolderId,
            name: folder.name,
            path: currentPath,
          });

          // Recursively create subfolders if enabled
          if (this.includeSubfolders) {
            const subFolders = await this.createFolderStructureOptimized(
              folder.id, 
              destFolderId, 
              currentPath,
              folderCache
            );
            folders.push(...subFolders);
          }
        }
      } catch (error) {
        console.error(`Error creating folder structure for ${sourceId}:`, error);
      }

      return folders;
    },

    async getExistingFiles(destFolderId, fileNames) {
      if (!fileNames.length) return new Map();
      
      try {
        // Build query to check for multiple files at once
        const nameQueries = fileNames.slice(0, 10).map(name => `name = '${name.replace(/'/g, "\\'")}'`);
        const query = `(${nameQueries.join(' or ')}) and '${destFolderId}' in parents and trashed = false`;
        
        const existingFiles = await this.google_drive.listFilesInPage(null, {
          q: query,
          fields: "files(id,name)",
          pageSize: 100,
        });

        return new Map(existingFiles.files?.map(f => [f.name, f.id]) || []);
      } catch (error) {
        console.error("Error checking existing files:", error);
        return new Map();
      }
    },

    async copyFileBatch(files, destFolderId, existingFileMap, results) {
      const copyTasks = files.map(async (file) => {
        try {
          let fileName = file.name;
          const fileExists = existingFileMap.has(fileName);

          if (fileExists) {
            if (this.conflictResolution === "skip") {
              return { success: true, skipped: true, file };
            } else if (this.conflictResolution === "rename") {
              const timestamp = Date.now();
              const extension = fileName.includes(".") ? fileName.split(".").pop() : "";
              const baseName = fileName.includes(".") ? fileName.substring(0, fileName.lastIndexOf(".")) : fileName;
              fileName = extension ? `${baseName}_copy_${timestamp}.${extension}` : `${baseName}_copy_${timestamp}`;
            } else if (this.conflictResolution === "replace") {
              await this.google_drive.deleteFile(existingFileMap.get(file.name));
            }
          }

          const copiedFile = await this.google_drive.copyFile(file.id, {
            requestBody: {
              name: fileName,
              parents: [destFolderId],
            },
          });

          return {
            success: true,
            originalName: file.name,
            newName: fileName,
            originalId: file.id,
            newId: copiedFile.id,
            size: file.size,
            mimeType: file.mimeType,
          };

        } catch (error) {
          return {
            success: false,
            file: file.name,
            error: error.message,
            type: "file_copy_error",
          };
        }
      });

      return await this.limitConcurrency(copyTasks, this.maxConcurrency);
    },

    async copyFilesOptimized(sourceId, destFolderId, results) {
      let pageToken = null;
      let totalProcessed = 0;
      
      do {
        try {
          const query = sourceId 
            ? `'${sourceId}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false`
            : `mimeType != 'application/vnd.google-apps.folder' and trashed = false`;

          const filesPage = await this.google_drive.listFilesInPage(pageToken, {
            q: query,
            fields: "nextPageToken,files(id,name,mimeType,size)",
            pageSize: Math.min(this.batchSize * 2, 100),
          });

          let filesToProcess = filesPage.files || [];
          
          // Filter by specific files if provided
          if (this.sourceFileIds?.length > 0) {
            filesToProcess = filesToProcess.filter(file => this.sourceFileIds.includes(file.id));
          }

          if (filesToProcess.length > 0) {
            // Get existing files in destination
            const fileNames = filesToProcess.map(f => f.name);
            const existingFileMap = await this.getExistingFiles(destFolderId, fileNames);

            // Process files in batches
            const batchResults = await this.processInBatches(
              filesToProcess,
              this.batchSize,
              async (batch, batchIndex) => {
                const copyResults = await this.copyFileBatch(batch, destFolderId, existingFileMap, results);
                
                // Process results
                const processedResults = [];
                for (const result of copyResults) {
                  if (result.status === 'fulfilled' && result.value) {
                    const value = result.value;
                    if (value.success && !value.skipped) {
                      results.filesCopied++;
                      results.copiedFiles.push(value);
                      processedResults.push(value);
                    } else if (value.skipped) {
                      processedResults.push(value);
                    }
                  } else if (result.status === 'rejected' || (result.value && !result.value.success)) {
                    const error = result.reason || result.value;
                    results.errors.push(error);
                  }
                }

                totalProcessed += batch.length;
                
                return processedResults;
              }
            );
          }

          pageToken = filesPage.nextPageToken;
        } catch (error) {
          console.error("Error in copyFilesOptimized:", error);
          results.errors.push({
            error: error.message,
            type: "batch_processing_error",
          });
          break;
        }
      } while (pageToken);

      return totalProcessed;
    },
  },
  async run({ $ }) {
    const results = {
      foldersCreated: 0,
      filesCopied: 0,
      errors: [],
      copiedFiles: [],
      createdFolders: [],
    };

    const startTime = Date.now();
    const timeoutMs = 270000; // 4.5 minutes to stay under 5-minute limit

    try {
      $.export("status", "Starting optimized bulk copy operation...");

      // Check if we're approaching timeout
      const checkTimeout = () => {
        if (Date.now() - startTime > timeoutMs) {
          throw new Error("Operation timeout - consider reducing batch size or using smaller file sets");
        }
      };

      const folderCache = new Map();

      if (this.preserveStructure && this.sourceFolderId) {
        checkTimeout();
        $.export("status", "Creating folder structure...");
        
        const folders = await this.createFolderStructureOptimized(
          this.sourceFolderId, 
          this.destinationFolderId,
          "",
          folderCache
        );
        
        results.foldersCreated = folders.length;
        results.createdFolders = folders.map(f => ({
          name: f.name,
          id: f.destId,
          path: f.path,
        }));

        checkTimeout();
        $.export("status", `Created ${folders.length} folders. Copying files...`);

        // Copy files in root folder first
        await this.copyFilesOptimized(this.sourceFolderId, this.destinationFolderId, results);

        checkTimeout();

        // Copy files in subfolders
        for (let i = 0; i < folders.length; i++) {
          checkTimeout();
          
          const folder = folders[i];
          $.export("progress", `Processing folder ${i + 1}/${folders.length}: ${folder.name}`);
          
          await this.copyFilesOptimized(folder.sourceId, folder.destId, results);
          
          // Small delay between folders
          await this.delay(50);
        }
      } else {
        checkTimeout();
        $.export("status", "Copying files to destination folder...");
        await this.copyFilesOptimized(this.sourceFolderId, this.destinationFolderId, results);
      }

      const duration = Math.round((Date.now() - startTime) / 1000);
      const summary = `Successfully copied ${results.filesCopied} files and created ${results.foldersCreated} folders in ${duration}s`;
      
      $.export("$summary", results.errors.length > 0 
        ? `${summary}. ${results.errors.length} errors occurred.`
        : summary
      );

      return {
        success: true,
        summary,
        duration: `${duration}s`,
        statistics: {
          foldersCreated: results.foldersCreated,
          filesCopied: results.filesCopied,
          errorsCount: results.errors.length,
          batchSize: this.batchSize,
          maxConcurrency: this.maxConcurrency,
        },
        details: {
          copiedFiles: results.copiedFiles.slice(0, 100), // Limit output size
          createdFolders: results.createdFolders,
          errors: results.errors.slice(0, 50), // Limit error output
        },
        timestamp: new Date().toISOString(),
      };

    } catch (error) {
      const duration = Math.round((Date.now() - startTime) / 1000);
      const partialSummary = `Partial copy completed: ${results.filesCopied} files and ${results.foldersCreated} folders copied in ${duration}s before error`;
      
      $.export("$summary", `${partialSummary}. Error: ${error.message}`);
      
      return {
        success: false,
        error: error.message,
        partialResults: {
          foldersCreated: results.foldersCreated,
          filesCopied: results.filesCopied,
          errorsCount: results.errors.length,
        },
        duration: `${duration}s`,
        timestamp: new Date().toISOString(),
      };
    }
  },
});