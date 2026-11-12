import type { FastifyPluginAsync } from 'fastify';
import type { OrdersService } from '../services/orders.service.js';

/**
 * Route de LABORATOIRE (TP5) : enregistrée uniquement avec LAB_MODE=1 et jamais en production.
 * Les commandes du fil rouge vivent en mémoire : cette route est la remise à zéro déterministe qui évite de redémarrer le service
 * (ORD-102 redevient CONFIRMED, ORD-103 reste SHIPPED). Elle ne touche ni RabbitMQ ni PostgreSQL.
 */
export function labRoutes(service: OrdersService): FastifyPluginAsync {
  return async (app) => {
    app.post('/api/lab/reset-orders', async () => {
      await service.resetOrdersForLab();
      return { reset: true, orders: (await service.list()).map((o) => ({ id: o.id, status: o.status })) };
    });
  };
}
