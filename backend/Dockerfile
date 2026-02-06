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

# Install necessary Python packages in one command.
# Keep this aligned with dl_lit_project runtime imports used by the backend scripts.
RUN pip install --no-cache-dir \
  google-genai google-api-core pikepdf pdfminer.six openai python-dotenv PyPDF2 rapidfuzz \
  requests beautifulsoup4 colorama bibtexparser lxml

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy package.json for faster, quieter installs
COPY package*.json ./

# Remove existing node_modules to force fresh install
RUN rm -rf node_modules

# Install dependencies with less output
RUN npm install --no-fund --no-audit --loglevel=error

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 4000

# Define the command to run the app
CMD ["npm", "start"]
