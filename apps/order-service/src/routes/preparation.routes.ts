import type { FastifyPluginAsync } from 'fastify';
import { createPreparationCommand, EXCHANGES, ROUTING_KEYS } from '@infal73/contracts';
import { PROBLEM_TYPES, sendProblem } from '../problems.js';
import { PublishError } from '../messaging/errors.js';
import type { PreparationPublisher } from '../messaging/preparation-publisher.js';
import type { OrdersService } from '../services/orders.service.js';

const PUBLISH_PROBLEM_DETAIL: Record<PublishError['kind'], string> = {
  'broker-unavailable': 'The message broker is unavailable. The preparation was NOT accepted; retry later.',
  unroutable: 'The preparation request could not be routed to a queue. It was NOT accepted.',
  nacked: 'The message broker refused the preparation request. It was NOT accepted.',
  'outcome-unknown':
    'The broker connection was lost before confirmation: the request may or may not have been queued. Retrying is safe (same operation).',
};

/**
 * POST /api/orders/:orderId/preparation — nouvelle route du TP4 (les routes historiques sont inchangées).
 * 202 uniquement si le broker a confirmé une publication routée ; jamais de faux 202.
 */
export function preparationRoutes(service: OrdersService, publisher: PreparationPublisher | undefined): FastifyPluginAsync {
  return async (app) => {
    app.post<{ Params: { orderId: string } }>('/api/orders/:orderId/preparation', async (request, reply) => {
      const { orderId } = request.params;
      const order = await service.get(orderId);
      if (!order) return sendProblem(request, reply, 404, PROBLEM_TYPES.orderNotFound, `Order ${orderId} does not exist.`);
      if (order.status === 'CANCELLED' || order.status === 'SHIPPED') {
        return sendProblem(request, reply, 409, PROBLEM_TYPES.invalidTransition, `An order in status ${order.status} cannot be prepared.`);
      }
      if (!publisher) {
        return sendProblem(request, reply, 503, PROBLEM_TYPES.upstreamUnavailable, 'Messaging is not configured on this instance.');
      }

      const command = createPreparationCommand(order.id);
      try {
        await publisher.publishConfirmed({
          exchange: EXCHANGES.commands,
          routingKey: ROUTING_KEYS.prepare,
          payload: command,
          mandatory: true,
          persistent: true,
          correlationId: request.id,
        });
      } catch (error) {
        if (error instanceof PublishError) {
          request.log.warn({ requestId: request.id, operationId: command.operationId, failure: error.kind }, 'preparation publish failed');
          return sendProblem(request, reply, 503, PROBLEM_TYPES.upstreamUnavailable, PUBLISH_PROBLEM_DETAIL[error.kind], {
            operationId: command.operationId,
          });
        }
        throw error;
      }
      request.log.info({ requestId: request.id, operationId: command.operationId, orderId: order.id }, 'preparation request published');
      return reply.code(202).send({ orderId: order.id, preparationStatus: 'PENDING' });
    });
  };
}
