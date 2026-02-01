import * as d3 from 'd3';

// Basic JavaScript for the Digital Library Frontend

console.log("Digital Library frontend script loaded.");

const GRAPH_API_URL = '/api/graph?max_nodes=200';
const GRAPH_SAMPLE_URL = '/graph-sample.json';
const GRAPH_COLORS = ['#2c7fb8', '#7fcdbb', '#fdae61', '#d7191c', '#8c6bb1', '#636363'];

function normalizeGraphPayload(payload) {
    if (!payload) {
        return { nodes: [], edges: [] };
    }
    const nodesRaw = Array.isArray(payload.nodes) ? payload.nodes : [];
    const edgesRaw = Array.isArray(payload.edges)
        ? payload.edges
        : Array.isArray(payload.links)
            ? payload.links
            : [];

    const nodes = nodesRaw.map((node, index) => ({
        ...node,
        id: node.id ?? node.work_id ?? node.openalex_id ?? `node-${index}`,
        group: node.type ?? node.source ?? 'work'
    }));

    const nodeIds = new Set(nodes.map((node) => node.id));
    const edges = edgesRaw
        .map((edge) => {
            const source = edge.source?.id ?? edge.source ?? edge.from ?? edge.source_work_id;
            const target = edge.target?.id ?? edge.target ?? edge.to ?? edge.referenced_work_id;
            return source && target ? { ...edge, source, target } : null;
        })
        .filter((edge) => edge && nodeIds.has(edge.source) && nodeIds.has(edge.target));

    return { nodes, edges };
}

function summarizeGraph(nodes, edges) {
    const typeCounts = new Map();
    nodes.forEach((node) => {
        const key = node.group || 'work';
        typeCounts.set(key, (typeCounts.get(key) || 0) + 1);
    });
    const typeSummary = Array.from(typeCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 4)
        .map(([key, count]) => `${key}: ${count}`)
        .join(', ');
    return {
        totalNodes: nodes.length,
        totalEdges: edges.length,
        typeSummary
    };
}

function renderGraph(container, nodes, edges) {
    container.innerHTML = '';
    const bounds = container.getBoundingClientRect();
    const width = bounds.width || 800;
    const height = bounds.height || 420;

    const svg = d3
        .select(container)
        .append('svg')
        .attr('width', width)
        .attr('height', height)
        .attr('viewBox', `0 0 ${width} ${height}`);

    const graphLayer = svg.append('g');

    const color = d3
        .scaleOrdinal()
        .domain(Array.from(new Set(nodes.map((node) => node.group))))
        .range(GRAPH_COLORS);

    const link = graphLayer
        .append('g')
        .attr('stroke', '#bdbdbd')
        .attr('stroke-opacity', 0.6)
        .selectAll('line')
        .data(edges)
        .join('line')
        .attr('stroke-width', 1);

    const simulation = d3
        .forceSimulation(nodes)
        .force(
            'link',
            d3.forceLink(edges).id((d) => d.id).distance(60).strength(0.5)
        )
        .force('charge', d3.forceManyBody().strength(-130))
        .force('center', d3.forceCenter(width / 2, height / 2));

    const drag = d3.drag()
        .on('start', (event, d) => {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        })
        .on('drag', (event, d) => {
            d.fx = event.x;
            d.fy = event.y;
        })
        .on('end', (event, d) => {
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        });

    const node = graphLayer
        .append('g')
        .attr('stroke', '#fff')
        .attr('stroke-width', 1.2)
        .selectAll('circle')
        .data(nodes)
        .join('circle')
        .attr('r', nodes.length > 150 ? 3.5 : 5)
        .attr('fill', (d) => color(d.group))
        .call(drag);

    node.append('title').text((d) => {
        const year = d.year ? ` (${d.year})` : '';
        return `${d.title || d.display_name || d.id}${year}`;
    });

    simulation.on('tick', () => {
        link
            .attr('x1', (d) => d.source.x)
            .attr('y1', (d) => d.source.y)
            .attr('x2', (d) => d.target.x)
            .attr('y2', (d) => d.target.y);
        node.attr('cx', (d) => d.x).attr('cy', (d) => d.y);
    });

    svg.call(
        d3.zoom().scaleExtent([0.3, 4]).on('zoom', (event) => {
            graphLayer.attr('transform', event.transform);
        })
    );
}

// --- Function to render bibliography data ---
// Moved to top level for broader scope
function renderBibliography(data, container) {
    container.innerHTML = ''; // Clear previous content
    if (!Array.isArray(data) || data.length === 0) {
        container.textContent = 'No bibliography files found.'; // Updated message
        return;
    }
    const ul = document.createElement('ul');
    data.forEach(filename => { // data is now an array of filenames
        const li = document.createElement('li');
        li.textContent = filename; // Display the filename directly
        ul.appendChild(li);
    });
    container.appendChild(ul);
}

// Drag and drop functionality for PDF files
document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const fileList = document.getElementById('file-list');
    
    // Store uploaded files
    const uploadedFiles = new Map();
    
    // Get consolidate button and status elements
    const consolidateButton = document.getElementById('consolidate-button');
    const consolidateStatus = document.getElementById('consolidate-status');

    // Add event listener for consolidate button
    if (consolidateButton && consolidateStatus) {
        consolidateButton.addEventListener('click', async () => {
            consolidateStatus.textContent = 'Consolidating...';
            consolidateStatus.style.color = 'orange';
            consolidateButton.disabled = true;

            try {
                const response = await fetch('/api/bibliographies/consolidate', {
                    method: 'POST'
                });

                const result = await response.json();

                if (response.ok) {
                    console.log('Consolidation successful:', result);
                    consolidateStatus.textContent = 'Consolidation successful! master_bibliography.json created.';
                    consolidateStatus.style.color = 'green';
                    loadInitialBibliographies(); // Refresh the list
                } else {
                    console.error('Consolidation failed:', result);
                    consolidateStatus.textContent = `Consolidation failed: ${result.message || 'Unknown error'}`;
                    consolidateStatus.style.color = 'red';
                }

            } catch (error) {
                console.error('Error triggering consolidation:', error);
                consolidateStatus.textContent = 'Error contacting server for consolidation.';
                consolidateStatus.style.color = 'red';
            } finally {
                consolidateButton.disabled = false;
            }
        });
    } else {
        console.warn('Consolidate button or status element not found.');
    }

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

            const bibContainer = document.getElementById('bib-list');
            statusSpan.textContent = 'Requesting extraction...';
            extractBibButton.disabled = true; // Disable button during processing

            try {
                // Add method: 'POST' to the fetch options
                const response = await fetch(`/api/extract-bibliography/${encodeURIComponent(backendFilename)}`, {
                    method: 'POST'
                });
                const result = await response.json();

                if (response.ok) {
                    statusSpan.textContent = 'Extraction started...';
                    // Display a message in the main bibliography box
                    if(bibContainer) bibContainer.textContent = 'Processing new file, list will refresh shortly...';

                    // Wait for a reasonable time (e.g., 15 seconds) for the script to likely complete
                    // In a production scenario, you might use WebSockets or more sophisticated polling
                    console.log('Waiting 15 seconds before refreshing bibliography list...');
                    setTimeout(() => {
                        console.log('Refreshing bibliography list now.');
                        loadInitialBibliographies(); // Reload the entire list
                         // Re-enable the button for the specific file if needed, or handle globally
                        // extractBibButton.disabled = false; 
                        // statusSpan.textContent = 'Extraction complete.'; // Or update based on refresh
                    }, 15000); // 15 seconds delay

                } else {
                    statusSpan.textContent = `Error: ${result.message || 'Unknown error'}`;
                    extractBibButton.disabled = false; // Re-enable on error
                }
            } catch (error) {
                console.error('Error requesting bibliography extraction:', error);
                statusSpan.textContent = 'Error starting extraction.';
                extractBibButton.disabled = false; // Re-enable on error
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
        
         // Poll and render bibliography JSON
         async function pollBibliography(fileName, container) {
             const baseName = fileName.replace(/\.[^/.]+$/, '');
             const url = `/api/bibliography/${encodeURIComponent(baseName)}`;
             try {
                 const response = await fetch(url);
                 if (!response.ok) {
                     throw new Error(`HTTP error! status: ${response.status}`);
                 }
                 const data = await response.json();
                 renderBibliography(data, container); // Use the rendering function
             } catch (error) {
                 console.error('Error polling bibliography:', error);
             }
         }

    } // Close addFileToList

    // Function to load all current bibliographies
    async function loadInitialBibliographies() {
        const bibContainer = document.getElementById('bib-list');
        if (!bibContainer) {
            console.error('Bibliography container #bib-list not found.');
            return;
        }
        bibContainer.textContent = 'Loading existing bibliographies...'; // Loading message

        try {
            const response = await fetch('/api/bibliographies/all-current');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            console.log('Loaded initial bibliographies:', data);
            renderBibliography(data, bibContainer); // Use the rendering function
        } catch (error) {
            console.error('Error loading initial bibliographies:', error);
            bibContainer.textContent = 'Error loading bibliographies.';
        }
    }

    // Call the initial load function
    loadInitialBibliographies();

    // --- Graph UI ---
    const graphContainer = document.getElementById('graph-canvas');
    const graphStatus = document.getElementById('graph-status');
    const graphStats = document.getElementById('graph-stats');
    const graphSource = document.getElementById('graph-source');
    const graphLoadButton = document.getElementById('graph-load');

    async function loadGraphData(source) {
        if (!graphContainer || !graphStatus || !graphStats) {
            return;
        }
        graphStatus.textContent = 'Loading...';
        graphStats.textContent = '';

        const url = source === 'api' ? GRAPH_API_URL : GRAPH_SAMPLE_URL;
        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const payload = await response.json();
            const { nodes, edges } = normalizeGraphPayload(payload);
            if (!nodes.length) {
                throw new Error('No nodes returned');
            }
            const summary = summarizeGraph(nodes, edges);
            renderGraph(graphContainer, nodes, edges);
            graphStatus.textContent = source === 'api' ? 'Loaded from API.' : 'Loaded sample data.';
            graphStats.textContent = `Nodes: ${summary.totalNodes} · Edges: ${summary.totalEdges}${
                summary.typeSummary ? ` · ${summary.typeSummary}` : ''
            }`;
        } catch (error) {
            if (source === 'api') {
                graphStatus.textContent = 'API unavailable. Falling back to sample data.';
                await loadGraphData('sample');
            } else {
                graphStatus.textContent = `Failed to load graph: ${error.message}`;
            }
        }
    }

    if (graphLoadButton) {
        graphLoadButton.addEventListener('click', () => {
            const source = graphSource ? graphSource.value : 'sample';
            loadGraphData(source);
        });
        loadGraphData('sample');
    }

    // Handle files being selected or dropped
    function handleFiles(files) {
        for (let i = 0; i < files.length; i++) {
            addFileToList(files[i]);
        }
    }

    fileInput.addEventListener('change', () => {
        handleFiles(fileInput.files);
        fileInput.value = '';
    });

    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

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

    dropZone.addEventListener('drop', handleDrop, false);

    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        handleFiles(files);
    }

    dropZone.addEventListener('click', () => {
        fileInput.click();
    });

    console.log("Drag and drop functionality initialized.");

    // --- WebSocket Connection ---
    let socket;

    function connectWebSocket() {
        // Use ws:// because we are likely running http locally
        // Adjust if your backend is served over https (wss://)
        // Assumes backend runs on port 4000 where the WebSocket server was attached
        const wsUrl = 'ws://localhost:4000'; 
        const logOutputDiv = document.getElementById('logOutput'); // Get the log div
        logOutputDiv.innerHTML = 'Connecting to server logs...<br>'; // Clear previous logs
        console.log(`Attempting to connect WebSocket to ${wsUrl}`);
        socket = new WebSocket(wsUrl);

        socket.onopen = () => {
            console.log('WebSocket connection established');
            logOutputDiv.innerHTML += 'Connection established. Waiting for logs...<br>';
        };

        socket.onmessage = (event) => {
            console.log('Message from server:', event.data);
            const logEntry = document.createElement('div');
            logEntry.textContent = event.data; // Display the raw message
            logOutputDiv.appendChild(logEntry);
            // Auto-scroll to the bottom
            logOutputDiv.scrollTop = logOutputDiv.scrollHeight;
        };

        socket.onclose = (event) => {
            console.log('WebSocket connection closed:', event);
            logOutputDiv.innerHTML += 'Log connection closed. Attempting to reconnect in 5 seconds...<br>';
            // Attempt to reconnect after a delay
            setTimeout(connectWebSocket, 5000); 
        };

        socket.onerror = (error) => {
            console.error('WebSocket error:', error);
            logOutputDiv.innerHTML += `WebSocket error: ${error.message || 'Unknown error'}. Check browser console.<br>`;
            // Don't automatically reconnect on error immediately, 
            // might spam if the server is down. Rely on onclose for retries.
        };
    }

    // Initial connection attempt
    connectWebSocket();
    // --- End WebSocket Connection ---
}); // end DOMContentLoaded
