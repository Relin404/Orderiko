FROM node:alpine AS development

RUN npm i -g pnpm

WORKDIR /usr/src/app

COPY package*.json ./

RUN pnpm install

COPY . .

RUN pnpm run build

FROM node:alpine AS production

ARG NODE_ENV=production
ARG NODE_ENV=${NODE_ENV}

WORKDIR /usr/src/app

COPY package*.json ./

RUN pnpm install --only=production

COPY . .

COPY --from=development /usr/src/app/dist ./dist

CMD ["node", "dist/apps/orders/main"]
