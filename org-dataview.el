;;; org-dataview.el --- Query and index Org frontmatter in SQLite database -*- lexical-binding: t; -*-

;; Author: Rasmus Sten
;; Version: 1.0.0
;; Keywords: org, sqlite, database, frontmatter
;; URL: https://github.com/yourname/org-dataview
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
Uses `org-id-locations-file' to find IDs."
  (org-dataview--debug "Reading Org IDs from %s" org-id-locations-file)
  (when (and (boundp 'org-id-locations-file)
             (file-exists-p org-id-locations-file))
    (org-dataview--debug "org-id-locations-file exists, proceeding to read")
    (with-temp-buffer
      (insert-file-contents org-id-locations-file)
      (let ((data (read (current-buffer))))
        (org-dataview--debug "Read %d ID entries from file" (length data))
        ;; Return alist (ID . relative-path)
        (mapcar (lambda (pair)
                  (let ((abs-path (car pair))
                        (id (cadr pair))
                        (rel-path (file-relative-name (car pair) org-directory)))
                    (org-dataview--debug "Using relative path for DB: %s" rel-path)
                    (cons id rel-path)))
                data)))))

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
  "If TEXT contains an Org link [[id:UUID][Display]], return (display . link).
Otherwise return (text . nil).
TEXT is the string to parse for Org links."
  (org-dataview--debug "Extracting link from text: %s" text)
  (if (string-match "\\[\\[\\([^]]+\\)\\]\\[\\([^]]+\\)\\]\\]" text)
      (let ((link (match-string 1 text))
            (display (match-string 2 text)))
        (org-dataview--debug "Found Org link: %s -> %s" display link)
        (cons display link))
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
     (if org-dataview-debug-mode
         (progn
           (message "ORG-DATAVIEW ERROR: Failed to insert %s = %s: %s" key display err)
           ;; Try to insert at least the key with empty value
           (condition-case err2
               (sqlite-execute db
                 (format "INSERT OR REPLACE INTO org_files (id, key, value, link) VALUES ('%s', '%s', '', NULL)"
                         (org-dataview--sqlite-escape-string id)
                         (org-dataview--sqlite-escape-string key)))
             (error
              (message "ORG-DATAVIEW CRITICAL: Cannot insert empty value: %s" err2)))))
     nil))) ; Return failure

(defun org-dataview--process-file (file id db)
  "Process FILE (relative to `org-directory`) and store its top frontmatter.
FILE is the relative file path, ID is the Org ID, DB is the SQLite connection.
Also stores a synthetic 'file' key with the filename and Org link [[id][title]].
Returns t if frontmatter was found and processed, nil otherwise."
  (let ((full-path (expand-file-name file org-directory))
        (found nil))
    (org-dataview--debug "Processing file: %s for ID: %s" full-path id)
    (when (file-exists-p full-path)
      (with-temp-buffer
        (insert-file-contents full-path)
        (org-mode)
        ;; -------------------------
        ;; 1. Parse frontmatter
        ;; -------------------------
        (let ((frontmatter (org-dataview--parse-frontmatter)))
          (org-dataview--debug "Frontmatter found: %s" frontmatter)
          (dolist (pair frontmatter)
            (let* ((key (downcase (car pair)))
                   (val (cdr pair))
                   (parsed (org-dataview--extract-link val))
                   (display (car parsed))
                   (link (cdr parsed)))
              (org-dataview--debug "Processing key: %s, value: %s" key val)
              (when (org-dataview--insert-frontmatter db id key display link)
                (setq found t)))))
        ;; -------------------------
        ;; 2. Insert synthetic 'file' row
        ;; -------------------------
        (let* ((title (file-name-nondirectory file))
               (link (format "[[%s][%s]]" id title)))
          (org-dataview--debug "Inserting synthetic 'file' row: %s → %s" title link)
          (when (org-dataview--insert-frontmatter db id "file" title link)
            (setq found t)))
        ;; -------------------------
        found))))

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
                   (found (org-dataview--process-file file id db)))
              
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
          (org-dataview--log "Scan complete - %d headings with frontmatter, %d without"
                       processed skipped))
      
      ;; Ensure DB is closed even if there's an error
      (sqlite-close db))))

;;;###autoload
(defun org-dataview-rescan (&optional root-dir)
  "Scan entire ROOT-DIR (default `org-directory`) for Org headings and update `org-id-locations-file`.
Stores paths relative to `org-directory`.
This function ensures all Org headings have IDs and updates the index.
ROOT-DIR is the directory to scan (defaults to org-directory)."
  (interactive)
  (if org-dataview-debug-mode
      (org-dataview--debug "Starting org-dataview-rescan")
    (org-dataview--log "Rescanning Org vault for IDs"))
  
  (let ((root (or root-dir org-directory)))
    (org-dataview--debug "Scanning directory: %s" root)
    
    (dolist (file (directory-files-recursively root "\\.org$"))
      (let ((already-open (get-file-buffer file))
            (buf (find-file-noselect file)))
        (org-dataview--debug "Processing file: %s (already open: %s)" file already-open)
        (with-current-buffer buf
          (org-mode)
          (org-map-entries #'org-id-get-create))
        (unless already-open
          (kill-buffer buf))))
    
    (org-dataview--debug "Finished scanning files, updating ID locations")
    
    ;; Update Org's native ID locations file
    (org-id-update-id-locations)
    
    ;; Convert paths to relative
    (org-dataview--write-relative-orgids)
    
    (if org-dataview-debug-mode
        (org-dataview--debug "Rescan complete")
      (org-dataview--log "Rescan complete: all headings now have Org IDs"))))

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
         (cond
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
  "Build SQL ORDER BY clause from SORT specification.
SORT is an alist of (column . direction) where direction is asc or desc.
Returns SQL ORDER BY clause as string, or nil if SORT is nil."
  (when sort
    (concat
     "ORDER BY "
     (mapconcat
      (lambda (it)
        (format "\"%s\" %s"
                (car it)
                (upcase (symbol-name (cdr it)))))
      sort
      ", "))))

(defun org-dataview--query (columns filter sort link-display)
  "Core query function. Execute SQL query and return results.
COLUMNS is a list of column names (strings).
FILTER is the filter specification (see `org-dataview--build-filter-clause').
SORT is the sort specification (see `org-dataview--build-sort-clause').
LINK-DISPLAY can be 'title to format title as Org link.
Returns (columns . rows) where rows is a list of row lists."
  (org-dataview--debug "ENTER org-dataview--query")
  (org-dataview--debug "link-display: %S" link-display)

  (unless org-dataview-default-location
    (error "org-dataview-default-location is not set"))

  (unless (file-exists-p org-dataview-default-location)
    (error "Database does not exist: %s" org-dataview-default-location))

  (let ((db (sqlite-open org-dataview-default-location)))
    (unwind-protect
        (let* ((where (org-dataview--build-filter-clause filter))
               (cols  (mapcar #'org-dataview--build-column-expr columns))
               (sortc (org-dataview--build-sort-clause sort))
               (sql   (format
                       "SELECT %s FROM org_files WHERE %s GROUP BY id %s"
                       (mapconcat #'identity cols ", ")
                       where
                       (or sortc ""))))

          (org-dataview--debug "SQL:\n%s" sql)

          (let* ((rows (sqlite-execute db sql))
                 (processed
                  (mapcar
                   (lambda (row)
                     (cl-loop
                      for val in row
                      for col in columns
                      collect
                      (cond
                       ;; Special override: if link-display is 'title and this is the title column
                       ((and (eq link-display 'title)
                             (string-equal col "title"))
                        (let ((file-link-index (cl-position "file.link" columns :test #'string-equal))
                              (title val))
                          (if (and file-link-index title)
                              (let ((file-link (nth file-link-index row)))
                                (if (and file-link (stringp file-link) (string-match "\\[\\[\\(.*?\\)\\]\\[" file-link))
                                    (format "[[%s][%s]]" (match-string 1 file-link) title)
                                  title))
                            title)))
                       ;; Default: return value as-is
                       (t
                        (or val "")))))
                   rows)))
            (cons columns processed)))

      (sqlite-close db)
      (org-dataview--debug "DB closed"))))

;; ==================== Public Query Interface ====================

;;;###autoload
(defun org-dataview-query (&rest args)
  "Run a query against the Org frontmatter database.

Keywords:
  :columns   (required) list of column names (strings)
  :filter    string | list
  :sort      alist ((\"col\" . asc|desc) ...)
  :link-display  'title (display title as [[uuid][title]])

Examples:
  (org-dataview-query :columns '(\"title\" \"author\") :filter \"book\")
  (org-dataview-query :columns '(\"title\" \"date\") 
                      :filter '(:and (\"type\" \"book\") (\"status\" \"read\"))
                      :sort '((\"date\" . desc)))"
  (org-dataview--debug "ENTER org-dataview-query")
  (org-dataview--debug "Args: %S" args)

  (let (columns filter sort link-display)
    ;; keyword parsing
    (while args
      (let ((key (pop args)))
        (pcase key
          (:columns       (setq columns (pop args)))
          (:filter        (setq filter (pop args)))
          (:sort          (setq sort (pop args)))
          (:link-display  (setq link-display (pop args)))
          (_ (error "Unknown keyword: %S" key)))))

    (unless columns
      (error ":columns is required"))

    (org-dataview--query columns filter sort link-display)))

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

(defun org-dataview-test-simple ()
  "Minimal sanity test.
Runs a simple query to verify the system is working."
  (interactive)
  (let ((res
         (org-dataview-query
          :columns '("title" "filetags")
          :filter "book")))
    (message "Rows: %d" (length (cdr res)))
    res))

;; ==================== Provide ====================

(provide 'org-dataview)

;;; org-dataview.el ends here
