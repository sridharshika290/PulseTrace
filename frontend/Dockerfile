FROM node:18 AS build
WORKDIR /app
COPY package.json package-lock.json* ./
COPY public ./public
COPY src ./src
RUN npm install --legacy-peer-deps
RUN npm run build

FROM nginx:stable-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["/usr/sbin/nginx", "-g", "daemon off;"]
