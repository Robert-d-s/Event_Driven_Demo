FROM node:22-slim

ENV NODE_ENV=production
WORKDIR /srv

COPY services/notification-service/package.json ./
RUN npm install --omit=dev

COPY services/notification-service/src ./src

CMD ["npm", "start"]
