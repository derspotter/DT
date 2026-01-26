# Use the official Node.js 23 Alpine image as the base
FROM node:23-alpine

# Install Python, pip, and virtualenv tools
RUN apk add --no-cache python3 py3-pip py3-virtualenv

# Create and activate a virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install necessary Python packages in one command
RUN pip install --no-cache-dir google-genai pikepdf pdfminer.six openai python-dotenv PyPDF2 rapidfuzz

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
