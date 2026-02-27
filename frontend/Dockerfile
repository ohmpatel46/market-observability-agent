FROM node:20-alpine

WORKDIR /frontend

COPY package.json ./
RUN npm install

COPY index.html ./index.html
COPY vite.config.js ./vite.config.js
COPY src ./src
COPY public ./public

EXPOSE 5173

CMD ["npm", "run", "dev"]
