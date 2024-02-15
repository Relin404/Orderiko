import { Logger, NotFoundException } from '@nestjs/common';
import { AbstractDocument } from './abstract.schema';
import {
  ClientSession,
  Connection,
  FilterQuery,
  Model,
  SaveOptions,
  Types,
  UpdateQuery,
} from 'mongoose';

export abstract class AbstractRepository<TDocument extends AbstractDocument> {
  protected abstract readonly logger: Logger;

  constructor(
    protected readonly model: Model<TDocument>,
    private readonly connection: Connection,
  ) {}

  async create(
    document: Omit<TDocument, '_id'>,
    options?: SaveOptions,
  ): Promise<TDocument> {
    const createdDocument = new this.model({
      ...document,
      _id: new Types.ObjectId(),
    });

    return (
      await createdDocument.save(options)
    ).toJSON() as unknown as TDocument;
  }

  async findOne(filterQuery: FilterQuery<TDocument>): Promise<TDocument> {
    // const document = await this.model.findOne(filterQuery, {}, { lean: true });
    const document = await this.model.findOne(filterQuery);

    if (!document) {
      this.logger.warn(`Document not found with filterQuery: ${filterQuery}`);
      throw new NotFoundException('Document not found.');
    }

    return document;
  }

  async find(filterQuery: FilterQuery<TDocument>): Promise<TDocument[]> {
    // const x = await this.model.find(filterQuery, {}, { lean: true });
    const docs = await this.model.find(filterQuery);
    return docs;
  }

  async findOneAndUpdate(
    filterQuery: FilterQuery<TDocument>,
    update: UpdateQuery<TDocument>,
  ) {
    const document = await this.model.findOneAndUpdate(filterQuery, update, {
      new: true,
    });

    if (!document) {
      this.logger.warn(`Document not found with filterQuery: ${filterQuery}`);
      throw new NotFoundException('Document not found.');
    }

    return document;
  }

  async upsert(
    filterQuery: FilterQuery<TDocument>,
    document: Partial<TDocument>,
  ) {
    return await this.model.findOneAndUpdate(filterQuery, document, {
      upsert: true,
      new: true,
    });
  }

  async delete(filterQuery: FilterQuery<TDocument>) {
    const document = await this.model.findOneAndDelete(filterQuery);

    if (!document) {
      this.logger.warn(`Document not found with filterQuery: ${filterQuery}`);
      throw new NotFoundException('Document not found.');
    }

    return document;
  }

  async startTransaction(): Promise<ClientSession> {
    const session = await this.connection.startSession();

    session.startTransaction();

    return session;
  }

  async endTransaction(
    session: ClientSession,
    isError: boolean,
  ): Promise<void> {
    if (isError) await session.abortTransaction();
    else await session.commitTransaction();

    await session.endSession();
  }
}
