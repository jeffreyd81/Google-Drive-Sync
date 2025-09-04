import google_drive from "@pipedream/google_drive";

export default defineComponent({
  name: "Compare Google Drive Folders",
  description: "Compare source and destination Google Drive folders to identify changes, additions, and deletions needed for incremental sync",
  type: "action",
  
  props: {
    google_drive,
    sourceFolderId: {
      propDefinition: [google_drive, "folderId"],
      label: "Source Folder",
      description: "The source folder to compare from",
    },
    destinationFolderId: {
      propDefinition: [google_drive, "folderId"], 
      label: "Destination Folder",
      description: "The destination folder to compare against",
    },
    includeSubfolders: {
      type: "boolean",
      label: "Include Subfolders",
      description: "Whether to recursively compare subfolders",
      optional: true,
      default: true,
    },
    compareContent: {
      type: "boolean", 
      label: "Compare Content",
      description: "Whether to compare file content using MD5 checksums (slower but more accurate)",
      optional: true,
      default: false,
    },
  },

  methods: {
    async getAllFilesInFolder(folderId, includeSubfolders = true, basePath = "") {
      const files = [];
      const folders = [];
      let pageToken = null;

      do {
        const response = await this.google_drive.listFilesInPage(pageToken, {
          q: `'${folderId}' in parents and trashed = false`,
          fields: "nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum,parents)",
          supportsAllDrives: true,
          includeItemsFromAllDrives: true,
        });

        for (const file of response.files || []) {
          const filePath = basePath ? `${basePath}/${file.name}` : file.name;
          const fileData = {
            id: file.id,
            name: file.name,
            path: filePath,
            mimeType: file.mimeType,
            size: file.size,
            modifiedTime: file.modifiedTime,
            md5Checksum: file.md5Checksum,
            isFolder: file.mimeType === "application/vnd.google-apps.folder",
          };

          if (fileData.isFolder) {
            folders.push(fileData);
            if (includeSubfolders) {
              const subFiles = await this.getAllFilesInFolder(
                file.id,
                includeSubfolders,
                filePath
              );
              files.push(...subFiles);
            }
          } else {
            files.push(fileData);
          }
        }

        pageToken = response.nextPageToken;
      } while (pageToken);

      return [...folders, ...files];
    },

    compareFiles(sourceFiles, destFiles, compareContent = false) {
      const sourceMap = new Map();
      const destMap = new Map();
      
      // Create maps for efficient lookup
      sourceFiles.forEach(file => sourceMap.set(file.path, file));
      destFiles.forEach(file => destMap.set(file.path, file));

      const additions = [];
      const modifications = [];
      const deletions = [];
      const unchanged = [];

      // Check for additions and modifications
      for (const [path, sourceFile] of sourceMap) {
        const destFile = destMap.get(path);
        
        if (!destFile) {
          additions.push(sourceFile);
        } else {
          const isModified = this.isFileModified(sourceFile, destFile, compareContent);
          if (isModified) {
            modifications.push({
              source: sourceFile,
              destination: destFile,
              reason: isModified,
            });
          } else {
            unchanged.push(sourceFile);
          }
        }
      }

      // Check for deletions
      for (const [path, destFile] of destMap) {
        if (!sourceMap.has(path)) {
          deletions.push(destFile);
        }
      }

      return {
        additions,
        modifications, 
        deletions,
        unchanged,
      };
    },

    isFileModified(sourceFile, destFile, compareContent = false) {
      const reasons = [];

      // Compare modification times
      if (new Date(sourceFile.modifiedTime) > new Date(destFile.modifiedTime)) {
        reasons.push("newer_modification_time");
      }

      // Compare file sizes
      if (sourceFile.size !== destFile.size) {
        reasons.push("different_size");
      }

      // Compare content checksums if enabled and available
      if (compareContent && sourceFile.md5Checksum && destFile.md5Checksum) {
        if (sourceFile.md5Checksum !== destFile.md5Checksum) {
          reasons.push("different_content");
        }
      }

      return reasons.length > 0 ? reasons : false;
    },

    generateSyncPlan(comparison) {
      const syncActions = [];

      // Add files that need to be created
      comparison.additions.forEach(file => {
        syncActions.push({
          action: "create",
          file,
          priority: file.isFolder ? 1 : 2, // Folders first
        });
      });

      // Add files that need to be updated
      comparison.modifications.forEach(({ source, destination, reason }) => {
        syncActions.push({
          action: "update",
          sourceFile: source,
          destinationFile: destination,
          reason,
          priority: source.isFolder ? 1 : 2,
        });
      });

      // Add files that need to be deleted
      comparison.deletions.forEach(file => {
        syncActions.push({
          action: "delete",
          file,
          priority: file.isFolder ? 3 : 2, // Delete files before folders
        });
      });

      // Sort by priority (folders first for creation, last for deletion)
      return syncActions.sort((a, b) => a.priority - b.priority);
    },
  },

  async run({ $ }) {
    try {
      $.export("$summary", "Starting folder comparison...");

      // Get all files from both folders
      const [sourceFiles, destFiles] = await Promise.all([
        this.getAllFilesInFolder(this.sourceFolderId, this.includeSubfolders),
        this.getAllFilesInFolder(this.destinationFolderId, this.includeSubfolders),
      ]);

      // Compare the files
      const comparison = this.compareFiles(sourceFiles, destFiles, this.compareContent);

      // Generate sync plan
      const syncPlan = this.generateSyncPlan(comparison);

      // Calculate statistics
      const stats = {
        sourceFiles: sourceFiles.length,
        destinationFiles: destFiles.length,
        additions: comparison.additions.length,
        modifications: comparison.modifications.length,
        deletions: comparison.deletions.length,
        unchanged: comparison.unchanged.length,
        totalSyncActions: syncPlan.length,
      };

      $.export("$summary", 
        `Comparison complete: ${stats.additions} additions, ${stats.modifications} modifications, ${stats.deletions} deletions`
      );

      return {
        statistics: stats,
        comparison: {
          additions: comparison.additions,
          modifications: comparison.modifications,
          deletions: comparison.deletions,
          unchanged: comparison.unchanged,
        },
        syncPlan,
        metadata: {
          sourceFolderId: this.sourceFolderId,
          destinationFolderId: this.destinationFolderId,
          includeSubfolders: this.includeSubfolders,
          compareContent: this.compareContent,
          comparisonTime: new Date().toISOString(),
        },
      };

    } catch (error) {
      throw new Error(`Folder comparison failed: ${error.message}`);
    }
  },
});