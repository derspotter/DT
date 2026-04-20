# Use a Debian-based Node.js image to support native modules like better-sqlite3
FROM node:22-bookworm-slim

# Install Python, pip, and build tools
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    python3-pip \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Create and activate a virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set the working directory inside the container
WORKDIR /usr/src/app

# Install Python dependencies from the canonical project requirements.
COPY dl_lit_project/requirements.txt /tmp/dl_lit_project-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dl_lit_project-requirements.txt

# Copy package.json for faster, quieter installs
COPY backend/package*.json ./

# Remove existing node_modules to force fresh install
RUN rm -rf node_modules

# Install dependencies with less output
RUN npm install --no-fund --no-audit --loglevel=error

# Copy the rest of the application code
COPY backend/ .

# Expose the port the app runs on
EXPOSE 4000

# Define the command to run the app
CMD ["npm", "start"]
