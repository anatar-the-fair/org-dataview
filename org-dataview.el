;;; org-dataview.el --- Query and index Org frontmatter in SQLite database -*- lexical-binding: t; -*-

;; Author: Rasmus Sten
;; Version: 1.0.1
;; Keywords: org, sqlite, database, frontmatter
;; URL: https://github.com/anatar-the-fair/org-dataview
;; Package-Requires: ((emacs "30.2")), have not tested other versions

;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:
;;
;; org-dataview provides a comprehensive system for indexing and querying
;; Org mode frontmatter in a SQLite database. It consists of two main parts:
;;
;; 1. INDEXING: Scan Org files with IDs and store their frontmatter in SQLite
;; 2. QUERYING: Query the database with flexible filters and sorting
;;
;; Key features:
;; - Stores all #+KEY: value pairs from Org files
;; - Supports Org links in frontmatter (extracts display text and link separately)
;; - Automatic ID management using Org's built-in ID system
;; - Relative path storage for portability
;; - Flexible query system with AND/OR filters, comparison operators
;; - Debug mode for troubleshooting
;;
;; Setup:
;; 1. Ensure Org IDs are set up: (setq org-id-locations-file "~/.emacs.d/.org-id-locations")
;; 2. Run M-x org-dataview-rescan to index your Org vault
;; 3. Query using M-x org-dataview-query or org-dataview-display
;;
;;; Code:

;; ==================== Dependencies ====================

(require 'cl-lib)
(require 'org)
(require 'sqlite)

;; ==================== Configuration ====================

(defvar org-dataview-default-location
  (expand-file-name "org_dataview.db" user-emacs-directory)
  "SQLite3 database file for storing Org frontmatter for dataview.
Default location is in user-emacs-directory.")

(defvar org-dataview-debug-mode nil
  "When non-nil, print detailed debug information.
Set to t to enable debug logging, nil for normal operation.
Can be toggled with `org-dataview-toggle-debug'.")

;; ==================== Debug Utilities ====================

(defun org-dataview--debug (format-string &rest args)
  "Print debug message if debug mode is enabled.
FORMAT-STRING is the format string, ARGS are the arguments."
  (when org-dataview-debug-mode
    (apply #'message (concat "ORG-DATAVIEW DEBUG: " format-string) args)))

(defun org-dataview--log (format-string &rest args)
  "Print normal log message.
FORMAT-STRING is the format string, ARGS are the arguments."
  (apply #'message (concat "ORG-DATAVIEW: " format-string) args))

(defun org-dataview--sqlite-escape-string (str)
  "Escape single quotes for SQLite.
STR is the string to escape. Returns escaped string."
  (if (stringp str)
      (replace-regexp-in-string "'" "''" str)
    ""))


;; ==================== Indexing Core Functions ====================

(defun org-dataview--read-orgids ()
  "Return alist of (ID . relative-path) for all headings with Org IDs.
Paths are relative to `org-directory`.
Uses `org-id-locations-file' to find IDs.
Filters duplicates to ensure each ID appears only once.
DOES NOT create or modify any IDs - only reads existing ones."
  (org-dataview--debug "Reading Org IDs from %s" org-id-locations-file)
  (when (and (boundp 'org-id-locations-file)
             (file-exists-p org-id-locations-file))
    (org-dataview--debug "org-id-locations-file exists, proceeding to read")
    (with-temp-buffer
      (insert-file-contents org-id-locations-file)
      (let ((data (read (current-buffer)))
            (id-table (make-hash-table :test 'equal))
            (result '()))
        (org-dataview--debug "Read %d ID entries from file" (length data))
        ;; Filter duplicates - keep only the first occurrence of each ID
        (dolist (pair data)
          (let ((abs-path (car pair))
                (id (cadr pair))
                (rel-path (file-relative-name (car pair) org-directory)))
            (unless (gethash id id-table)
              (puthash id t id-table)
              (push (cons id rel-path) result))))
        (nreverse result)))))

(defun org-dataview--parse-frontmatter ()
  "Parse consecutive #+KEY: lines at the top of the buffer.
Skips anything before the first #+KEY: line. Returns alist (key . value).
Returns nil if no frontmatter found."
  (goto-char (point-min))
  (org-dataview--debug "Searching for frontmatter in buffer")
  (when (re-search-forward "^#\\+" nil t)
    (beginning-of-line)
    (org-dataview--debug "Found first frontmatter line at position %d" (point))
    (let ((lines '()))
      (while (looking-at "^#\\+\\([^:]+\\):[ \t]*\\(.*\\)$")
        (let ((key (match-string 1))
              (value (match-string 2)))
          (org-dataview--debug "Parsed frontmatter key: %s = %s" key value)
          (push (cons (downcase key) value) lines))
        (forward-line 1))
      (let ((result (nreverse lines)))
        (org-dataview--debug "Total frontmatter entries parsed: %d" (length result))
        result))))

(defun org-dataview--extract-link (text)
  "If TEXT contains an Org link [[id:UUID][Display]], return (display . full-link).
Otherwise return (text . nil).
TEXT is the string to parse for Org links.
Returns the FULL link including brackets, not just the id: part."
  (org-dataview--debug "Extracting link from text: %s" text)
  (if (string-match "\\(\\[\\[[^]]+\\]\\[[^]]+\\]\\]\\)" text)
      (let ((full-link (match-string 1 text)))
        ;; Now extract just the display text for the value
        (string-match "\\[\\[[^]]+\\]\\[\\([^]]+\\)\\]\\]" full-link)
        (let ((display (match-string 1 full-link)))
          (org-dataview--debug "Found Org link: %s -> %s" display full-link)
          (cons display full-link)))
    (progn
      (org-dataview--debug "No Org link found in text")
      (cons text nil))))

(defun org-dataview--ensure-table (db)
  "Ensure SQLite3 table exists for storing Org frontmatter.
DB is the SQLite database connection."
  (org-dataview--debug "Ensuring table exists in database")
  (sqlite-execute db
                  "CREATE TABLE IF NOT EXISTS org_files (
        id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        link TEXT,
        PRIMARY KEY (id, key)
    );")
  (org-dataview--debug "Table org_files created or already exists"))

(defun org-dataview--insert-frontmatter (db id key display link)
  "Insert frontmatter into database with proper error handling.
DB is the SQLite connection, ID is the Org ID, KEY is the frontmatter key,
DISPLAY is the display text, LINK is the Org link (optional).
Returns t on success, nil on failure."
  (condition-case err
      (progn
        (if link
            (sqlite-execute db
                            (format "INSERT OR REPLACE INTO org_files (id, key, value, link) VALUES ('%s', '%s', '%s', '%s')"
                                    (org-dataview--sqlite-escape-string id)
                                    (org-dataview--sqlite-escape-string key)
                                    (org-dataview--sqlite-escape-string display)
                                    (org-dataview--sqlite-escape-string link)))
          (sqlite-execute db
                          (format "INSERT OR REPLACE INTO org_files (id, key, value, link) VALUES ('%s', '%s', '%s', NULL)"
                                  (org-dataview--sqlite-escape-string id)
                                  (org-dataview--sqlite-escape-string key)
                                  (org-dataview--sqlite-escape-string display))))
        t) ; Return success
    (error
     ;; Always log errors, not just in debug mode
     (message "ORG-DATAVIEW ERROR: Failed to insert %s = %s: %s" key display err)
     ;; Try to insert at least the key with empty value
     (condition-case err2
         (progn
           (sqlite-execute db
                           (format "INSERT OR REPLACE INTO org_files (id, key, value, link) VALUES ('%s', '%s', '', NULL)"
                                   (org-dataview--sqlite-escape-string id)
                                   (org-dataview--sqlite-escape-string key)))
           (message "ORG-DATAVIEW: Inserted empty value for %s" key)
           t) ; Still return success
       (error
        (message "ORG-DATAVIEW CRITICAL: Cannot insert empty value for %s: %s" key err2)
        nil))))) ; Return failure

(defun org-dataview--process-file (file id db)
  "Process FILE (relative to `org-directory`) and store its top frontmatter.
FILE is the relative file path, ID is the Org ID, DB is the SQLite connection.
First clears any existing data for this ID to prevent duplicates.
Returns t if frontmatter was found and stored, nil otherwise.
DOES NOT create or modify any IDs - only reads existing frontmatter."
  (let ((full-path (expand-file-name file org-directory))
        (found nil))
    (org-dataview--debug "Processing file: %s for ID: %s" full-path id)
    (when (file-exists-p full-path)
      ;; First, delete any existing data for this ID to prevent duplicates
      (sqlite-execute db
                      (format "DELETE FROM org_files WHERE id='%s'"
                              (org-dataview--sqlite-escape-string id)))
      (with-temp-buffer
        (insert-file-contents full-path)
        ;; Use fundamental-mode to avoid any Org mode hooks that might create IDs
        (fundamental-mode)
        ;; -------------------------
        ;; 1. Parse frontmatter
        ;; -------------------------
        (let ((frontmatter (org-dataview--parse-frontmatter)))
          (if frontmatter
              (progn
                (org-dataview--debug "Frontmatter found: %s" frontmatter)
                (dolist (pair frontmatter)
                  (let* ((key (downcase (car pair)))
                         (val (cdr pair))
                         (parsed (org-dataview--extract-link val))
                         (display (car parsed))
                         (link (cdr parsed)))
                    (org-dataview--debug "Processing key: %s, value: %s" key val)
                    (when (org-dataview--insert-frontmatter db id key display link)
                      (setq found t))))
                ;; -------------------------
                ;; 2. Insert synthetic 'file' row (filename only)
                ;; -------------------------
                (let* ((filename (file-name-nondirectory file))  ; Just the filename
                       (link (format "[[id:%s][%s]]" id filename)))
                  (org-dataview--debug "Inserting synthetic 'file' row: %s → %s" filename link)
                  (when (org-dataview--insert-frontmatter db id "file" filename link)
                    (setq found t)))
                ;; -------------------------
                ;; 3. Insert 'file.path' row (relative path)
                ;; -------------------------
                (org-dataview--debug "Inserting 'file.path' row: %s" file)
                (when (org-dataview--insert-frontmatter db id "file.path" file nil)
                  (setq found t)))
            (org-dataview--debug "No frontmatter found in file %s" file)))
        found))))

;; ==================== Public Indexing Interface ====================

;;;###autoload
(defun org-dataview-scan-orgid-locations (&optional db-file)
  "Scan all headings listed in Org IDs and store their frontmatter in DB-FILE.
If DB-FILE is nil, uses `org-dataview-default-location'.
Displays progress in percent even if debug mode is off.
This function updates the database with current frontmatter from all Org IDs.
DOES NOT create or modify any IDs - only reads existing ones from org-id-locations-file."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-scan-orgid-locations")
    (org-dataview--log "Starting scan of Org IDs"))
  
  (let* ((db-file (or db-file org-dataview-default-location))
         (db (sqlite-open db-file))
         (orgids (org-dataview--read-orgids))
         (total (length orgids))
         (processed 0)
         (skipped 0)
         (errors 0)
         (last-percent -1)) ; Track last printed percent
    (org-dataview--debug "Database file: %s" db-file)
    (org-dataview--debug "Total Org IDs to process: %d" total)
    
    (unwind-protect
        (progn
          (org-dataview--ensure-table db)
          
          (dolist (pair orgids)
            (let* ((id (car pair))
                   (file (cdr pair))
                   (found (condition-case err
                              (org-dataview--process-file file id db)
                            (error
                             (message "ORG-DATAVIEW ERROR processing %s: %s" file err)
                             (setq errors (1+ errors))
                             nil))))
              
              (if found
                  (setq processed (1+ processed))
                (setq skipped (1+ skipped)))
              
              ;; Show progress in percent
              (let* ((done (+ processed skipped errors))
                     (percent (if (> total 0)
                                  (/ (* done 100.0) total)
                                100))
                     (percent-int (floor percent)))
                (when (/= percent-int last-percent)
                  (setq last-percent percent-int)
                  (message "ORG-DATAVIEW: Indexing progress: %d%% (%d/%d)"
                           percent-int done total)))))
          
          ;; Final summary
          (org-dataview--log "Scan complete - %d with frontmatter, %d without, %d errors"
                             processed skipped errors))
      
      ;; Ensure DB is closed even if there's an error
      (sqlite-close db))))

;;;###autoload
(defun org-dataview-rescan-files (&optional root-dir)
  "Scan ROOT-DIR for Org files and collect metadata for existing IDs.
Does NOT create new IDs - only scans existing ones.
Does NOT call org-id-update-id-locations to avoid triggering ID creation.
ROOT-DIR defaults to org-directory."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-rescan-files")
    (org-dataview--log "Scanning for existing IDs (no new IDs will be created)"))
  
  ;; REMOVED: org-id-update-id-locations call that could trigger ID creation
  ;; Simply scan the IDs that already exist in org-id-locations-file
  (org-dataview-scan-orgid-locations)
  
  (if org-dataview-debug-mode
      (org-dataview--debug "Rescan-files complete")
    (org-dataview--log "Rescan-files complete: existing IDs scanned")))

;;;###autoload
(defun org-dataview-rescan-all (&optional root-dir)
  "Scan ALL Org files in ROOT-DIR and collect metadata for existing IDs.
Does NOT create new IDs - only scans existing ones.
Does NOT call org-id-update-id-locations to avoid triggering ID creation.
ROOT-DIR defaults to org-directory."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-rescan-all")
    (org-dataview--log "Rescanning ALL files for existing IDs (no new IDs will be created)"))
  
  ;; REMOVED: org-id-update-id-locations call that could trigger ID creation
  ;; Simply scan the IDs that already exist in org-id-locations-file
  (org-dataview-scan-orgid-locations)
  
  (if org-dataview-debug-mode
      (org-dataview--debug "Rescan-all complete")
    (org-dataview--log "Rescan-all complete: existing IDs scanned")))

(defun org-dataview--write-relative-orgids ()
  "Convert all absolute paths in `org-id-locations-file` to relative paths.
This makes the ID file portable across different machines/filesystems.
DOES NOT create or modify any IDs - only converts path formats."
  (org-dataview--debug "Writing relative Org IDs to %s" org-id-locations-file)
  (when (and (boundp 'org-id-locations-file)
             (file-exists-p org-id-locations-file))
    (org-dataview--debug "org-id-locations-file exists, proceeding to convert paths")
    (with-temp-buffer
      (insert-file-contents org-id-locations-file)
      (let* ((data (read (current-buffer)))
             (rel-data
              (mapcar (lambda (pair)
                        (let ((abs-path (car pair))
                              (id (cadr pair)))
                          (list (file-relative-name abs-path org-directory) id)))
                      data)))
        (org-dataview--debug "Converted %d paths to relative format" (length rel-data))
        (with-temp-file org-id-locations-file
          (prin1 rel-data (current-buffer)))))))

;; ==================== Public Indexing Interface ====================

;;;###autoload
(defun org-dataview-scan-orgid-locations (&optional db-file)
  "Scan all headings listed in Org IDs and store their frontmatter in DB-FILE.
If DB-FILE is nil, uses `org-dataview-default-location'.
Displays progress in percent even if debug mode is off.
This function updates the database with current frontmatter from all Org IDs."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-scan-orgid-locations")
    (org-dataview--log "Starting scan of Org IDs"))
  
  (let* ((db-file (or db-file org-dataview-default-location))
         (db (sqlite-open db-file))
         (orgids (org-dataview--read-orgids))
         (total (length orgids))
         (processed 0)
         (skipped 0)
         (errors 0)
         (last-percent -1)) ; Track last printed percent
    (org-dataview--debug "Database file: %s" db-file)
    (org-dataview--debug "Total Org IDs to process: %d" total)
    
    (unwind-protect
        (progn
          (org-dataview--ensure-table db)
          
          (dolist (pair orgids)
            (let* ((id (car pair))
                   (file (cdr pair))
                   (found (condition-case err
                              (org-dataview--process-file file id db)
                            (error
                             (message "ORG-DATAVIEW ERROR processing %s: %s" file err)
                             (setq errors (1+ errors))
                             nil))))
              
              (if found
                  (setq processed (1+ processed))
                (setq skipped (1+ skipped)))
              
              ;; Show progress in percent
              (let* ((done (+ processed skipped errors))
                     (percent (if (> total 0)
                                  (/ (* done 100.0) total)
                                100))
                     (percent-int (floor percent)))
                (when (/= percent-int last-percent)
                  (setq last-percent percent-int)
                  (message "ORG-DATAVIEW: Indexing progress: %d%% (%d/%d)"
                           percent-int done total)))))
          
          ;; Final summary
          (org-dataview--log "Scan complete - %d with frontmatter, %d without, %d errors"
                             processed skipped errors))
      
      ;; Ensure DB is closed even if there's an error
      (sqlite-close db))))

;;;###autoload
(defun org-dataview-rescan-files (&optional root-dir)
  "Scan ROOT-DIR for Org files and collect metadata for existing IDs.
Does NOT create new IDs - only scans existing ones.
ROOT-DIR defaults to org-directory."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-rescan-files")
    (org-dataview--log "Scanning for existing IDs (no new IDs will be created)"))
  
  ;; Update Org's ID locations
  (org-id-update-id-locations)
  (org-dataview--write-relative-orgids)
  
  ;; Now scan using the updated IDs
  (org-dataview-scan-orgid-locations)
  
  (if org-dataview-debug-mode
      (org-dataview--debug "Rescan-files complete")
    (org-dataview--log "Rescan-files complete: existing IDs scanned")))

;;;###autoload
(defun org-dataview-rescan-all (&optional root-dir)
  "Scan ALL Org files in ROOT-DIR and collect metadata for existing IDs.
Does NOT create new IDs - only scans existing ones.
ROOT-DIR defaults to org-directory."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-rescan-all")
    (org-dataview--log "Rescanning ALL files for existing IDs (no new IDs will be created)"))
  
  ;; Update Org's ID locations
  (org-id-update-id-locations)
  (org-dataview--write-relative-orgids)
  
  ;; Now scan using the updated IDs
  (org-dataview-scan-orgid-locations)
  
  (if org-dataview-debug-mode
      (org-dataview--debug "Rescan-all complete")
    (org-dataview--log "Rescan-all complete: existing IDs scanned")))

(defun org-dataview--write-relative-orgids ()
  "Convert all absolute paths in `org-id-locations-file` to relative paths.
This makes the ID file portable across different machines/filesystems."
  (org-dataview--debug "Writing relative Org IDs to %s" org-id-locations-file)
  (when (and (boundp 'org-id-locations-file)
             (file-exists-p org-id-locations-file))
    (org-dataview--debug "org-id-locations-file exists, proceeding to convert paths")
    (with-temp-buffer
      (insert-file-contents org-id-locations-file)
      (let* ((data (read (current-buffer)))
             (rel-data
              (mapcar (lambda (pair)
                        (let ((abs-path (car pair))
                              (id (cadr pair)))
                          (list (file-relative-name abs-path org-directory) id)))
                      data)))
        (org-dataview--debug "Converted %d paths to relative format" (length rel-data))
        (with-temp-file org-id-locations-file
          (prin1 rel-data (current-buffer)))))))

;; ==================== Query Core Functions ====================

(defun org-dataview--build-filter-clause (filter)
  "Build SQL WHERE clause from FILTER specification.
FILTER can be:
- nil or empty string: returns '1=1' (match all)
- string: searches in 'filetags' column with LIKE
- list: (:and filter1 filter2 ...) or (:or filter1 filter2 ...)
- list: (key value) where value can have comparison operators: >, <, >=, <=
  Special keys: 'file.path', 'file.name' for file path filtering
Returns SQL WHERE clause as string."
  (org-dataview--debug "Building filter: %S" filter)
  (cond
   ((null filter) "1=1")

   ((and (stringp filter) (string-empty-p filter))
    "1=1")

   ((stringp filter)
    (let ((safe (replace-regexp-in-string "'" "''" filter)))
      (format
       "id IN (SELECT id FROM org_files WHERE key='filetags' AND value LIKE '%%%s%%')"
       safe)))

   ((listp filter)
    (pcase (car filter)
      (:and
       (mapconcat #'org-dataview--build-filter-clause (cdr filter) " AND "))
      (:or
       (mapconcat #'org-dataview--build-filter-clause (cdr filter) " OR "))
      (_
       (let* ((key   (replace-regexp-in-string "'" "''" (car filter)))
              (value (cadr filter))
              (safev (replace-regexp-in-string "'" "''" value)))
         
         ;; Order matters! Check most specific first
         (cond
          ;; 1. Special file path filters
          ((string-equal key "file.path")
           (format "id IN (SELECT id FROM org_files WHERE key='file.path' AND value LIKE '%s')"
                   safev))
          
          ((string-equal key "file.name")
           (format "id IN (SELECT id FROM org_files WHERE key='file' AND value = '%s')"
                   safev))
          
          ;; 2. Numeric comparisons (check prefixes)
          ((string-prefix-p ">=" value)
           (format "id IN (SELECT id FROM org_files WHERE key='%s' AND CAST(value AS INTEGER) >= %s)"
                   key (substring value 2)))
          
          ((string-prefix-p "<=" value)
           (format "id IN (SELECT id FROM org_files WHERE key='%s' AND CAST(value AS INTEGER) <= %s)"
                   key (substring value 2)))
          
          ((string-prefix-p ">" value)
           (format "id IN (SELECT id FROM org_files WHERE key='%s' AND CAST(value AS INTEGER) > %s)"
                   key (substring value 1)))
          
          ((string-prefix-p "<" value)
           (format "id IN (SELECT id FROM org_files WHERE key='%s' AND CAST(value AS INTEGER) < %s)"
                   key (substring value 1)))
          
          ;; 3. String with LIKE wildcard (%)
          ((and (stringp value) (string-match-p "%" value))
           (format "id IN (SELECT id FROM org_files WHERE key='%s' AND value LIKE '%s')"
                   key safev))
          
          ;; 4. Exact match (default - MUST BE LAST)
          (t
           (format "id IN (SELECT id FROM org_files WHERE key='%s' AND value='%s')"
                   key safev)))))))

   (t
    (error "Invalid filter: %S" filter))))


(defun org-dataview--build-column-expr (col)
  "Build SQL expression for column COL.
If COL ends with '.link', get the link field for that key.
Otherwise get the value field.
COL is the column name (string)."
  (if (string-suffix-p ".link" col)
      ;; Get link for this key
      (let ((key (substring col 0 (- (length col) 5))))
        (format "MAX(CASE WHEN key='%s' THEN link END) AS \"%s\""
                (replace-regexp-in-string "'" "''" key)
                col))
    ;; Get value for this key
    (let ((safe (replace-regexp-in-string "'" "''" col)))
      (format "MAX(CASE WHEN key='%s' THEN value END) AS \"%s\""
              safe col))))

(defun org-dataview--build-sort-clause (sort)
  "Build SQL ORDER BY clause from SORT specification."
  (message "DEBUG org-dataview--build-sort-clause: sort=%S (type: %s)" sort (type-of sort))
  (when sort
    (message "DEBUG: sort is truthy but not nil, processing...")
    (concat
     "ORDER BY "
     (mapconcat
      (lambda (it)
        (message "DEBUG: Processing sort item: %S" it)
        (format "\"%s\" %s"
                (car it)
                (upcase (symbol-name (cdr it)))))
      sort
      ", "))))

(defun org-dataview--query (columns column-aliases filter sort link-display)
  "Core query function."
  (message "=== DEBUG START org-dataview--query ===")
  (message "Parameters: columns=%S, column-aliases=%S, filter=%S, sort=%S, link-display=%S"
           columns column-aliases filter sort link-display)
  
  (unless org-dataview-default-location
    (error "org-dataview-default-location is not set"))

  (unless (file-exists-p org-dataview-default-location)
    (error "Database does not exist: %s" org-dataview-default-location))

  (let ((db (sqlite-open org-dataview-default-location)))
    (unwind-protect
        (let* ((where (org-dataview--build-filter-clause filter))
               ;; If link-display is 'title, we need both title and file.link
               (need-title (equal link-display 'title))
               (need-file-link (equal link-display 'title))
               (all-columns (append columns
                                    (when need-title (unless (member "title" columns) '("title")))
                                    (when need-file-link (unless (member "file.link" columns) '("file.link")))))
               (cols (mapcar #'org-dataview--build-column-expr all-columns))
               (sortc (org-dataview--build-sort-clause sort))
               (sql (format
                     "SELECT %s FROM org_files WHERE %s GROUP BY id %s"
                     (mapconcat #'identity cols ", ")
                     where
                     (or sortc ""))))

          (message "DEBUG SQL: %s" sql)

          (let* ((sql-result (sqlite-execute db sql))
                 (rows (cond
                        ((numberp sql-result)  ; sqlite-execute returns 0 for no results
                         '())
                        ((listp sql-result)    ; Normal case: list of rows
                         sql-result)
                        (t
                         (error "Unexpected result from sqlite-execute: %S" sql-result))))
                 ;; Apply column aliases to original columns
                 (display-columns
                  (mapcar (lambda (col)
                            (or (cdr (assoc col column-aliases)) col))
                          columns))
                 (processed
                  (mapcar
                   (lambda (row)
                     (let ((row-as-list (append row nil))) ; Convert vector to list if needed
                       ;; Process each requested column
                       (mapcar 
                        (lambda (col)
                          ;; Find this column's value in the row
                          (let ((val (cl-loop for i from 0
                                              for c in all-columns
                                              when (string-equal c col)
                                              return (nth i row-as-list))))
                            (cond
                             ;; If link-display is 'title AND this is the "title" column
                             ((and (equal link-display 'title)
                                   (string-equal col "title"))
                              (let ((title val)
                                    ;; Find the file.link value
                                    (file-link (cl-loop for i from 0
                                                        for c in all-columns
                                                        when (string-equal c "file.link")
                                                        return (nth i row-as-list))))
                                (if (and file-link title (stringp file-link))
                                    (cond
                                     ;; Extract UUID from [[id:UUID][filename]]
                                     ((string-match "\\[\\[id:\\([^]]+\\)\\]\\[" file-link)
                                      (let ((uuid (match-string 1 file-link)))
                                        (format "[[id:%s][%s]]" uuid title)))
                                     ;; Fallback: return title only
                                     (t title))
                                  ;; No file.link found, return title only
                                  (or title ""))))
                             ;; Any other column: return as-is
                             (t
                              (or val "")))))
                        columns)))
                   rows)))
            ;; DEBUG: Show what we're returning
            (message "DEBUG: Returning %d rows: %S" (length processed) processed)
            (cons display-columns processed)))

      (sqlite-close db))))

;; ==================== Public Query Interface ====================

;;;###autoload
(defun org-dataview-query (&rest args)
  "Run a query against the Org frontmatter database."
  (message "=== DEBUG START org-dataview-query ===")
  (message "Args received: %S" args)
  
  (let (columns column-aliases filter sort link-display)
    ;; Initialize defaults
    (setq column-aliases nil
          filter nil
          sort nil
          link-display nil)
    
    ;; keyword parsing
    (message "Parsing keywords...")
    (while args
      (let ((key (pop args)))
        (message "Processing key: %S" key)
        (pcase key
          (:columns         (setq columns (pop args)) (message "Set columns to: %S" columns))
          (:column-aliases  (setq column-aliases (pop args)) (message "Set column-aliases to: %S" column-aliases))
          (:filter          (setq filter (pop args)) (message "Set filter to: %S" filter))
          (:sort            (setq sort (pop args)) 
                           (message "Set sort to: %S (type: %s)" sort (type-of sort))
                           ;; Ensure sort is properly formatted
                           (when (and sort (not (listp sort)))
                             (message "WARNING: sort is not a list, setting to nil")
                             (setq sort nil)))
          (:link-display    (setq link-display (pop args)) (message "Set link-display to: %S" link-display))
          (_ (error "Unknown keyword: %S" key)))))
    
    (message "Final values: columns=%S, column-aliases=%S, filter=%S, sort=%S, link-display=%S"
             columns column-aliases filter sort link-display)
    
    (unless columns
      (error ":columns is required"))
    
    (message "=== DEBUG END org-dataview-query ===")
    (org-dataview--query columns column-aliases filter sort link-display)))

;;;###autoload
(defun org-dataview-display (columns filter &optional sort link-display)
  "Display query results as an Org list or table in a buffer.
COLUMNS is a comma-separated string of column names.
FILTER is the filter string.
SORT is an alist of (column . direction) where direction is asc or desc.
LINK-DISPLAY can be 'title to format title as Org link.
Opens results in *Org Dataview Results* buffer."
  (interactive
   (list (read-from-minibuffer "Columns (comma separated): ")
         (read-from-minibuffer "Filter: ")))
  (let* ((cols (mapcar #'string-trim (split-string columns ",")))
         (results (org-dataview-query
                   :columns cols
                   :filter filter
                   :sort sort
                   :link-display link-display))
         (rows (cdr results)))
    (with-current-buffer (get-buffer-create "*Org Dataview Results*")
      (erase-buffer)
      (org-mode)
      ;; If single column, display as list
      (if (= (length cols) 1)
          (dolist (row rows)
            (insert "- " (or (car row) "") "\n")
            (insert "- " (or (car row) "") "\n"))
        ;; Multiple columns, display as table
        (insert "| " (mapconcat #'identity (car results) " | ") " |\n")
        (insert "|-|\n")
        (dolist (row rows)
          (insert "| " (mapconcat (lambda (x) (or x "")) row " | ") " |\n")))
      (goto-char (point-min))
      (pop-to-buffer (current-buffer)))))

;; ==================== Utility Functions ====================

;;;###autoload
(defun org-dataview-toggle-debug ()
  "Toggle debug mode on/off.
When debug mode is on, detailed logging messages are displayed."
  (interactive)
  (setq org-dataview-debug-mode (not org-dataview-debug-mode))
  (message "ORG-DATAVIEW debug mode %s" (if org-dataview-debug-mode "enabled" "disabled")))

;;;###autoload
(defun org-dataview-inspect ()
  "Show contents of the database in a readable format.
Displays all entries in the database in a formatted message buffer."
  (interactive)
  (let* ((db-file org-dataview-default-location)
         (db (sqlite-open db-file))
         (count 0))
    (unwind-protect
        (progn
          (org-dataview--log "Inspecting database: %s" db-file)
          
          (sqlite-select db "SELECT id, key, value, link FROM org_files ORDER BY id, key" nil
                         (lambda (row)
                           (setq count (1+ count))
                           (let ((id (nth 0 row))
                                 (key (nth 1 row))
                                 (value (nth 2 row))
                                 (link (nth 3 row)))
                             (message "%-36s %-15s %s%s"
                                      id key value
                                      (if link (format " → %s" link) "")))))
          
          (if (> count 0)
              (org-dataview--log "Found %d entries" count)
            (org-dataview--log "Database is empty")))
      
      (sqlite-close db))))

;;;###autoload
(defun org-dataview-clear-database ()
  "Clear all data from the database.
Prompts for confirmation before deleting all data."
  (interactive)
  (when (yes-or-no-p "Are you sure you want to clear the entire database?")
    (let* ((db-file org-dataview-default-location)
           (db (sqlite-open db-file)))
      (unwind-protect
          (progn
            (sqlite-execute db "DELETE FROM org_files")
            (org-dataview--log "Database cleared"))
        (sqlite-close db)))))

;;;###autoload
(defun org-dataview-test-connection ()
  "Test database connection and insertion with sample data.
Useful for verifying that the database setup is working correctly."
  (interactive)
  (let* ((db-file org-dataview-default-location)
         (db (sqlite-open db-file)))
    (unwind-protect
        (progn
          (org-dataview--ensure-table db)
          
          ;; Test with various value types
          (dolist (test '(("test-id-1" "title" "Simple Title" nil)
                          ("test-id-1" "date" "[2025-01-01 Wed 10:00]" nil)
                          ("test-id-1" "tags" ":tag1:tag2:" nil)
                          ("test-id-2" "dob" "1903" nil)
                          ("test-id-2" "dod" "1950" nil)
                          ("test-id-3" "link" "Display Text" "id:12345")))
            (let ((id (nth 0 test))
                  (key (nth 1 test))
                  (value (nth 2 test))
                  (link (nth 3 test)))
              (condition-case err
                  (progn
                    (org-dataview--insert-frontmatter db id key value link)
                    (message "✓ Test passed: %s = %s" key value))
                (error
                 (message "✗ Test failed: %s = %s → %s" key value err))))))
      
      (sqlite-close db))))

;;;###autoload
(defun org-dataview-scan-file-ids (&optional root-dir)
  "Scan Org files for file-level IDs (#+ID: value) and collect their metadata.
Creates IDs for files that don't have them at the file level.
ROOT-DIR defaults to org-directory."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-scan-file-ids")
    (org-dataview--log "Scanning for file-level IDs"))
  
  (let ((root (or root-dir org-directory))
        (files-processed 0)
        (file-ids-found 0))
    (org-dataview--debug "Scanning directory: %s" root)
    
    (dolist (file (directory-files-recursively root "\\.org$"))
      (let ((already-open (get-file-buffer file))
            (buf (find-file-noselect file)))
        (org-dataview--debug "Processing file: %s" file)
        (with-current-buffer buf
          (org-mode)
          (save-excursion
            (goto-char (point-min))
            ;; Look for file-level ID (#+ID: value)
            (if (re-search-forward "^#\\+ID:[ \t]+\\([^ \t\n]+\\)" nil t)
                ;; File has file-level ID
                (let ((file-id (match-string 1)))
                  (org-dataview--debug "File-level ID found: %s" file-id)
                  (setq file-ids-found (1+ file-ids-found))
                  ;; Add to org-id-locations if not already there
                  (unless (org-id-find-id-in-file file-id file)
                    (org-id-add-location file-id file)))
              ;; No file-level ID, create one at the file level (NOT in headings)
              (org-dataview--debug "No file-level ID, creating one")
              (let ((file-id (org-id-new)))
                (save-excursion
                  (goto-char (point-min))
                  (insert (format "#+ID: %s\n" file-id)))
                (org-id-add-location file-id file)
                (setq file-ids-found (1+ file-ids-found)))))
          (setq files-processed (1+ files-processed)))
        (unless already-open
          (kill-buffer buf))))
    
    ;; Update Org's ID locations
    (org-id-update-id-locations)
    (org-dataview--write-relative-orgids)
    
    ;; Now scan using the updated IDs
    (org-dataview-scan-orgid-locations)
    
    (if org-dataview-debug-mode
        (org-dataview--debug "Scan-file-ids complete")
      (org-dataview--log "Scan-file-ids complete: %d files processed, %d file-level IDs found/created" 
                         files-processed file-ids-found))))

;; ==================== Provide ====================

(provide 'org-dataview)

;;; org-dataview.el ends here
