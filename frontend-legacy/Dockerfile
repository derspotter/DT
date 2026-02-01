# Use the official Node.js 23 Alpine image as the base
FROM node:23-alpine

# Set environment variable to indicate we're in Docker
ENV DOCKER_ENV=true
ENV NODE_ENV=development

# Set the working directory inside the container
WORKDIR /app

# Copy package.json (and package-lock.json if available)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of your frontend source code
COPY . .

# Expose the port Vite uses (default is 3000 now)
EXPOSE 3000

# Run Vite in development mode (hot-reload enabled)
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]