import { Injectable, Logger } from '@nestjs/common';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { AbstractRepository } from '@app/common';
import { Order } from './schemas/order.schema';
import { Connection, Model } from 'mongoose';

@Injectable()
export class OrdersRepository extends AbstractRepository<Order> {
  protected readonly logger = new Logger(OrdersRepository.name);

  constructor(
    @InjectModel(Order.name) orderModel: Model<Order>,
    @InjectConnection() connection: Connection,
  ) {
    super(orderModel, connection);
  }
}
