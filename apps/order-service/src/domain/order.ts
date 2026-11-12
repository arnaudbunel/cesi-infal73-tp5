export const ORDER_STATUSES = ['CREATED', 'CONFIRMED', 'SHIPPED', 'CANCELLED'] as const;
export type OrderStatus = (typeof ORDER_STATUSES)[number];

export interface OrderItem {
  productId: string;
  quantity: number;
}

export interface Order {
  id: string;
  customerId: string;
  items: OrderItem[];
  status: OrderStatus;
  /** Propriétaire (identité démo). Interne : jamais exposé par l'API REST. */
  ownerId: string;
  /** Entier >= 0, +1 par transition effectivement enregistrée (jamais sur refus, rejeu ou consultation). */
  sequence: number;
  createdAt: string;
  updatedAt: string;
}

export interface NewOrderInput {
  customerId: string;
  items: OrderItem[];
}

const TRANSITIONS: Record<OrderStatus, readonly OrderStatus[]> = {
  CREATED: ['CONFIRMED', 'CANCELLED'],
  CONFIRMED: ['SHIPPED', 'CANCELLED'],
  SHIPPED: [],
  CANCELLED: [],
};

export function canTransition(from: OrderStatus, to: OrderStatus): boolean {
  return TRANSITIONS[from].includes(to);
}

export function isOrderStatus(value: unknown): value is OrderStatus {
  return typeof value === 'string' && (ORDER_STATUSES as readonly string[]).includes(value);
}
