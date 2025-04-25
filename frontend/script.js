// Basic JavaScript for the Digital Library Frontend

console.log("Digital Library frontend script loaded.");

// Drag and drop functionality for PDF files
document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const fileList = document.getElementById('file-list');
    
    // Store uploaded files
    const uploadedFiles = new Map();
    
    // Convert bytes to readable format
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }
    
    // Add file to the list
    function addFileToList(file) {
        // Check if it's a PDF
        if (!file.type.match('application/pdf')) {
            alert('Only PDF files are allowed');
            return;
        }
        
        // Check if file is already in the list
        if (uploadedFiles.has(file.name)) {
            alert(`File "${file.name}" is already in the list`);
            return;
        }
        
        // Add to map
        uploadedFiles.set(file.name, file);
        
        // Create list item for the file
        const listItem = document.createElement('li');
        listItem.dataset.localFilename = file.name; // Store local name for removal
        
        // Add a status area
        const statusSpan = document.createElement('span');
        statusSpan.className = 'file-status';
        statusSpan.textContent = 'Pending upload...';
        listItem.appendChild(statusSpan); // Add status early

        // File info container
        const fileInfo = document.createElement('div');
        fileInfo.className = 'file-info';
        
        // Icon (using inline SVG for simplicity)
        const icon = document.createElement('img');
        icon.src = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='24' height='24' viewBox='0 0 24 24' fill='none' stroke='%23e74c3c' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'%3E%3C/path%3E%3Cpolyline points='14 2 14 8 20 8'%3E%3C/polyline%3E%3C/svg%3E";
        icon.alt = "PDF file";
        icon.style.width = '24px';
        icon.style.height = '24px';
        icon.style.marginRight = '10px';
        
        // File name and size
        const fileDetails = document.createElement('div');
        fileDetails.className = 'file-details';
        
        const fileName = document.createElement('div');
        fileName.className = 'file-name';
        fileName.textContent = file.name;
        
        const fileSize = document.createElement('div');
        fileSize.className = 'file-size';
        fileSize.textContent = formatFileSize(file.size);
        
        fileDetails.appendChild(fileName);
        fileDetails.appendChild(fileSize);
        
        fileInfo.appendChild(icon);
        fileInfo.appendChild(fileDetails);
        
        // Action buttons container
        const fileActions = document.createElement('div');
        fileActions.className = 'file-actions';
        
        // Remove button
        const removeButton = document.createElement('button');
        removeButton.className = 'remove-btn';
        removeButton.textContent = 'Remove';
        removeButton.addEventListener('click', () => {
            // TODO: Add logic to cancel ongoing upload/extraction if possible?
            uploadedFiles.delete(listItem.dataset.localFilename); // Use local name for map key
            listItem.remove();
        });
        
        // Extract Bibliography button
        const extractBibButton = document.createElement('button');
        extractBibButton.className = 'extract-bib-btn'; // New class name
        extractBibButton.textContent = 'Extract Bibliography';
        extractBibButton.disabled = true; // Disabled until upload completes

        extractBibButton.addEventListener('click', async () => {
            const backendFilename = listItem.dataset.backendFilename;
            if (!backendFilename) {
                alert('File has not been uploaded successfully yet, cannot extract.');
                return;
            }

            // Update status and disable button during API call
            statusSpan.textContent = 'Starting extraction...';
            extractBibButton.disabled = true;
            extractBibButton.textContent = 'Starting...';
            removeButton.disabled = true; // Also disable remove during extraction start

            try {
                console.log(`Requesting bibliography extraction for backend file: ${backendFilename}`);
                
                // Call the new backend endpoint
                const response = await fetch(`/api/extract-bibliography/${backendFilename}`, { 
                    method: 'POST',
                    // No body needed, filename is in URL
                });

                // Check for 202 Accepted or other success codes
                if (!response.ok && response.status !== 202) { 
                    const errorData = await response.text(); 
                    console.error('Backend error response:', errorData);
                    throw new Error(`HTTP error! status: ${response.status}`);
                }

                const result = await response.json(); // Get the { message: '...' } response
                console.log('Backend response:', result);
                statusSpan.textContent = result.message || 'Extraction started (background).'; 
                extractBibButton.textContent = 'Extraction Running'; // Indicate background process
                // Keep button disabled? Or allow re-trigger? For now, keep disabled.

            } catch (error) {
                console.error('Error starting bibliography extraction:', error);
                statusSpan.textContent = 'Error starting extraction.';
                alert('Error starting bibliography extraction. Please check console.'); 
                extractBibButton.disabled = false; // Re-enable on error
                extractBibButton.textContent = 'Extract Bibliography'; 
            } finally {
                // Re-enable remove button once API call finishes (success or fail)
                 removeButton.disabled = false; 
            }
        });
        
        fileActions.appendChild(removeButton);
        fileActions.appendChild(extractBibButton); // Add the updated button
        
        listItem.appendChild(fileInfo);
        listItem.appendChild(fileActions);
        
        fileList.appendChild(listItem);

        // AUTO UPLOAD 
        // Function to handle the actual upload
        async function uploadFile(fileToUpload, itemElement, statusElement, buttonElement) {
            const formData = new FormData();
            formData.append('file', fileToUpload);
            
            try {
                statusElement.textContent = 'Uploading...';
                buttonElement.disabled = true;
                removeButton.disabled = true; // Disable remove during upload

                const response = await fetch('/api/process_pdf', { // Using the modified upload endpoint
                    method: 'POST',
                    body: formData
                });

                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`Upload failed: ${response.status} ${errorText}`);
                }

                const result = await response.json();
                if (result.filename) {
                    itemElement.dataset.backendFilename = result.filename; // STORE THE FILENAME!
                    statusElement.textContent = 'Ready for extraction.';
                    buttonElement.disabled = false; // Enable extraction button
                    console.log(`File ${fileToUpload.name} uploaded as ${result.filename}`);
                } else {
                    throw new Error('Backend did not return a filename.');
                }
            } catch (error) {
                console.error(`Error uploading ${fileToUpload.name}:`, error);
                statusElement.textContent = `Upload failed: ${error.message}`; 
                itemElement.classList.add('upload-failed'); // Add class for styling?
                 // Keep extract button disabled
            } finally {
                removeButton.disabled = false; // Re-enable remove button
            }
        }
        
        // Trigger upload immediately after adding elements to list
        uploadFile(file, listItem, statusSpan, extractBibButton);
        // --- END AUTO UPLOAD ---
    }
    
    // Handle files being selected or dropped
    function handleFiles(files) {
        for (let i = 0; i < files.length; i++) {
            addFileToList(files[i]);
        }
    }
    
    // File input change event (when files are selected via the dialog)
    fileInput.addEventListener('change', () => {
        handleFiles(fileInput.files);
        fileInput.value = '';
    });
    
    // Prevent default drag behaviors
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
    });
    
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }
    
    // Highlight drop zone when file is dragged over it
    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, highlight, false);
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, unhighlight, false);
    });
    
    function highlight() {
        dropZone.classList.add('highlight');
    }
    
    function unhighlight() {
        dropZone.classList.remove('highlight');
    }
    
    // Handle dropped files
    dropZone.addEventListener('drop', handleDrop, false);
    
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        handleFiles(files);
    }
    
    // Open file picker when the drop zone is clicked
    dropZone.addEventListener('click', () => {
        fileInput.click();
    });
    
    console.log("Drag and drop functionality initialized.");
});