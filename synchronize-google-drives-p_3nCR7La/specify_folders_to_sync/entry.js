import google_drive from "@pipedream/google_drive"

export default defineComponent({
  name: "Configure Google Drive Folder Sync",
  description: "Configure folders to sync between Google Drive locations. Specify source folders using IDs, names, or paths for bulk synchronization setup.",
  type: "action",
  props: {
    google_drive,
    source_drive: {
      propDefinition: [
        google_drive,
        "watchedDrive"
      ],
      label: "Source Drive",
      description: "Select the source drive (My Drive or a Shared Drive)"
    },
    folder_selection_method: {
      type: "string",
      label: "Folder Selection Method",
      description: "Choose how you want to specify folders for synchronization",
      options: [
        { label: "Select Individual Folders", value: "individual" },
        { label: "Specify Folder IDs", value: "folder_ids" },
        { label: "Search by Folder Names", value: "folder_names" },
        { label: "Sync Entire Drive Root", value: "entire_drive" }
      ],
      default: "individual"
    },
    source_folders: {
      type: "string[]",
      label: "Source Folders",
      description: "Select the folders you want to sync from the source drive",
      optional: true,
      async options({ prevContext }) {
        if (this.folder_selection_method !== "individual") return [];
        
        const { nextPageToken } = prevContext;
        return this.google_drive.listFolderOptions(nextPageToken, {
          driveId: this.source_drive === "My Drive" ? undefined : this.source_drive,
          supportsAllDrives: true,
          includeItemsFromAllDrives: true
        });
      }
    },
    source_folder_ids: {
      type: "string[]",
      label: "Source Folder IDs",
      description: "Enter Google Drive folder IDs to sync (one per line)",
      optional: true
    },
    source_folder_names: {
      type: "string[]", 
      label: "Source Folder Names",
      description: "Enter folder names to search for and sync (exact matches only)",
      optional: true
    },
    destination_parent_folder: {
      type: "string",
      label: "Destination Parent Folder",
      description: "Select the parent folder where synced folders will be created (leave empty for drive root)",
      optional: true,
      async options({ prevContext }) {
        const { nextPageToken } = prevContext;
        return this.google_drive.listFolderOptions(nextPageToken, {
          driveId: this.source_drive === "My Drive" ? undefined : this.source_drive,
          supportsAllDrives: true,
          includeItemsFromAllDrives: true
        });
      }
    },
    sync_subfolders: {
      type: "boolean",
      label: "Sync Subfolders",
      description: "Include all subfolders within the selected folders",
      optional: true,
      default: true
    },
    preserve_folder_structure: {
      type: "boolean",
      label: "Preserve Folder Structure", 
      description: "Maintain the original folder hierarchy in the destination",
      optional: true,
      default: true
    },
    sync_permissions: {
      type: "boolean",
      label: "Sync Folder Permissions",
      description: "Copy folder sharing permissions to the destination (when possible)",
      optional: true,
      default: false
    }
  },
  methods: {
    async listSourceFolders(opts = {}) {
      const driveId = this.source_drive === "My Drive" ? undefined : this.source_drive;
      return await this.google_drive.listFilesInPage(null, {
        q: "mimeType = 'application/vnd.google-apps.folder' and trashed != true",
        driveId,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
        fields: "files(id,name,parents)",
        ...opts,
      });
    },
    async getSourceFile(fileId, opts = {}) {
      return await this.google_drive.getFile(fileId, {
        supportsAllDrives: true,
        fields: "id,name,parents,mimeType",
        ...opts,
      });
    },
    async findSourceFolders(folderName) {
      const driveId = this.source_drive === "My Drive" ? undefined : this.source_drive;
      const { files = [] } = await this.google_drive.listFilesInPage(null, {
        q: `mimeType = 'application/vnd.google-apps.folder' and name = '${folderName}' and trashed != true`,
        driveId,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
        fields: "files(id,name,parents)",
      });
      return files;
    }
  },
  async run({ $ }) {
    const syncConfig = {
      source: {
        drive: this.source_drive,
        driveId: this.source_drive === "My Drive" ? null : this.source_drive
      },
      destination: {
        drive: this.source_drive, // Using same drive for now since we can only auth with one account
        driveId: this.source_drive === "My Drive" ? null : this.source_drive,
        parentFolder: this.destination_parent_folder || null
      },
      options: {
        syncSubfolders: this.sync_subfolders,
        preserveFolderStructure: this.preserve_folder_structure,
        syncPermissions: this.sync_permissions
      },
      folders: []
    };

    // Process folders based on selection method
    if (this.folder_selection_method === "entire_drive") {
      // Get all folders from the source drive root
      const { files: folders } = await this.listSourceFolders();
      
      syncConfig.folders = folders.map(folder => ({
        id: folder.id,
        name: folder.name,
        source: "drive_root"
      }));
      
    } else if (this.folder_selection_method === "individual" && this.source_folders?.length) {
      // Get detailed info for selected folders
      for (const folderId of this.source_folders) {
        try {
          const folder = await this.getSourceFile(folderId);
          syncConfig.folders.push({
            id: folder.id,
            name: folder.name,
            source: "manual_selection",
            parents: folder.parents
          });
        } catch (error) {
          console.log(`Error getting folder ${folderId}:`, error.message);
        }
      }
      
    } else if (this.folder_selection_method === "folder_ids" && this.source_folder_ids?.length) {
      // Validate and get folder details for provided IDs
      for (const folderId of this.source_folder_ids) {
        try {
          const folder = await this.getSourceFile(folderId);
          if (folder.mimeType === "application/vnd.google-apps.folder") {
            syncConfig.folders.push({
              id: folder.id,
              name: folder.name,
              source: "folder_id",
              parents: folder.parents
            });
          }
        } catch (error) {
          console.log(`Invalid folder ID ${folderId}:`, error.message);
        }
      }
      
    } else if (this.folder_selection_method === "folder_names" && this.source_folder_names?.length) {
      // Search for folders by name
      for (const folderName of this.source_folder_names) {
        try {
          const folders = await this.findSourceFolders(folderName);
          
          folders.forEach(folder => {
            syncConfig.folders.push({
              id: folder.id,
              name: folder.name,
              source: "folder_name_search",
              parents: folder.parents
            });
          });
        } catch (error) {
          console.log(`Error searching for folder "${folderName}":`, error.message);
        }
      }
    }

    // Validate configuration
    if (syncConfig.folders.length === 0) {
      throw new Error("No valid folders found with the current configuration. Please check your folder selection settings.");
    }

    $.export("$summary", `Successfully configured sync for ${syncConfig.folders.length} folder${syncConfig.folders.length === 1 ? '' : 's'} from ${this.source_drive}`);

    return {
      syncConfiguration: syncConfig,
      folderCount: syncConfig.folders.length,
      folderList: syncConfig.folders.map(f => ({
        id: f.id,
        name: f.name
      })),
      ready: true,
      timestamp: new Date().toISOString()
    };
  }
})