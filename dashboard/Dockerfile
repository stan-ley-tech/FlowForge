FROM node:20-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG VITE_FLOWFORGE_API_URL=http://localhost:8080
ENV VITE_FLOWFORGE_API_URL=$VITE_FLOWFORGE_API_URL
RUN npm run build

FROM nginx:alpine
COPY --from=build /src/dist /usr/share/nginx/html
EXPOSE 80
