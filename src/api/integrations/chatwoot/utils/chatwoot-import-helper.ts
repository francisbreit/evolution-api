import { inbox } from '@figuro/chatwoot-sdk';
import { proto } from '@whiskeysockets/baileys';

import { InstanceDto } from '../../../../api/dto/instance.dto';
import { ChatwootRaw, ContactRaw, MessageRaw } from '../../../../api/models';
import { Chatwoot, configService } from '../../../../config/env.config';
import { Logger } from '../../../../config/logger.config';
import { postgresClient } from '../libs/postgres.client';
import { ChatwootService } from '../services/chatwoot.service';

type ChatwootUser = {
  user_type: string;
  user_id: number;
};

type FksChatwoot = {
  phone_number: string;
  contact_id: string;
  conversation_id: string;
};

type firstLastTimestamp = {
  first: number;
  last: number;
};

type IWebMessageInfo = Omit<proto.IWebMessageInfo, 'key'> & Partial<Pick<proto.IWebMessageInfo, 'key'>>;

class ChatwootImport {
  private logger = new Logger(ChatwootImport.name);
  private repositoryMessagesCache = new Map<string, Set<string>>();
  private historyMessages = new Map<string, MessageRaw[]>();
  private historyContacts = new Map<string, ContactRaw[]>();

  public getRepositoryMessagesCache(instance: InstanceDto) {
    return this.repositoryMessagesCache.has(instance.instanceName)
      ? this.repositoryMessagesCache.get(instance.instanceName)
      : null;
  }

  public setRepositoryMessagesCache(instance: InstanceDto, repositoryMessagesCache: Set<string>) {
    this.repositoryMessagesCache.set(instance.instanceName, repositoryMessagesCache);
  }

  public deleteRepositoryMessagesCache(instance: InstanceDto) {
    this.repositoryMessagesCache.delete(instance.instanceName);
  }

  public addHistoryMessages(instance: InstanceDto, messagesRaw: MessageRaw[]) {
    const actualValue = this.historyMessages.has(instance.instanceName)
      ? this.historyMessages.get(instance.instanceName)
      : [];
    this.historyMessages.set(instance.instanceName, actualValue.concat(messagesRaw));
  }

  public addHistoryContacts(instance: InstanceDto, contactsRaw: ContactRaw[]) {
    const actualValue = this.historyContacts.has(instance.instanceName)
      ? this.historyContacts.get(instance.instanceName)
      : [];
    this.historyContacts.set(instance.instanceName, actualValue.concat(contactsRaw));
  }

  public deleteHistoryMessages(instance: InstanceDto) {
    this.historyMessages.delete(instance.instanceName);
  }

  public deleteHistoryContacts(instance: InstanceDto) {
    this.historyContacts.delete(instance.instanceName);
  }

  public clearAll(instance: InstanceDto) {
    this.deleteRepositoryMessagesCache(instance);
    this.deleteHistoryMessages(instance);
    this.deleteHistoryContacts(instance);
  }

  public getHistoryMessagesLenght(instance: InstanceDto) {
    return this.historyMessages.get(instance.instanceName)?.length ?? 0;
  }

  public async insertLabel(instanceName: string, accountId: number) {
    const pgClient = postgresClient.getChatwootConnection();
    const sqlCheckLabel = `
      SELECT 1 FROM labels WHERE title = $1 AND account_id = $2
    `;
    const sqlInsertLabel = `
      INSERT INTO labels (title, description, color, show_on_sidebar, account_id, created_at, updated_at)
      VALUES ($1, 'fonte origem do contato', '#2BB32F', TRUE, $2, NOW(), NOW())
      RETURNING *
    `;

    try {
      const checkResult = await pgClient.query(sqlCheckLabel, [instanceName, accountId]);
      if (checkResult.rowCount === 0) {
        const result = await pgClient.query(sqlInsertLabel, [instanceName, accountId]);
        return result.rows[0];
      } else {
        this.logger.info(`Label with title ${instanceName} already exists for account_id ${accountId}`);
        return null;
      }
    } catch (error) {
      this.logger.error(`Error on insert label: ${error.toString()}`);
    }
  }

  public async insertTag(instanceName: string, totalContacts: number) {
    const pgClient = postgresClient.getChatwootConnection();
    const sqlInsertTag = `
      INSERT INTO tags (name, taggings_count)
      VALUES ($1, $2)
      RETURNING id
    `;
    try {
      const result = await pgClient.query(sqlInsertTag, [instanceName, totalContacts]);
      return result.rows[0].id;
    } catch (error) {
      this.logger.error(`Error on insert tag: ${error.toString()}`);
    }
  }

  public async insertTaggings(tagId: number, contacts: ContactRaw[], createdAt: Date) {
    const pgClient = postgresClient.getChatwootConnection();
    const sqlInsertTaggings = `
      INSERT INTO taggings (tag_id, taggable_type, taggable_id, tagger_type, tagger_id, context, created_at)
      VALUES ($1, 'Contact', $2, NULL, NULL, 'labels', $3)
    `;

    try {
      const values = contacts.map(contact => [tagId, contact.id, createdAt]);
      for (const value of values) {
        await pgClient.query(sqlInsertTaggings, value);
      }
    } catch (error) {
      this.logger.error(`Error on insert taggings: ${error.toString()}`);
    }
  }

  public async importHistoryContacts(instance: InstanceDto, provider: ChatwootRaw) {
    try {
      if (this.getHistoryMessagesLenght(instance) > 0) {
        return;
      }

      const pgClient = postgresClient.getChatwootConnection();

      let totalContactsImported = 0;

      const contacts = this.historyContacts.get(instance.instanceName) || [];
      if (contacts.length === 0) {
        return 0;
      }

      let contactsChunk: ContactRaw[] = this.sliceIntoChunks(contacts, 3000);
      await this.insertLabel(instance.instanceName, Number(provider.account_id));

      const tagId = await this.insertTag(instance.instanceName, contacts.length);
      const createdAt = new Date();

      while (contactsChunk.length > 0) {
        let sqlInsert = `INSERT INTO contacts
          (name, phone_number, account_id, identifier, created_at, updated_at) VALUES `;
        const bindInsert = [provider.account_id];

        for (const contact of contactsChunk) {
          bindInsert.push(contact.pushName);
          const bindName = `$${bindInsert.length}`;

          bindInsert.push(`+${contact.id.split('@')[0]}`);
          const bindPhoneNumber = `$${bindInsert.length}`;

          bindInsert.push(contact.id);
          const bindIdentifier = `$${bindInsert.length}`;

          sqlInsert += `(${bindName}, ${bindPhoneNumber}, $1, ${bindIdentifier}, NOW(), NOW()),`;
        }
        if (sqlInsert.slice(-1) === ',') {
          sqlInsert = sqlInsert.slice(0, -1);
        }
        sqlInsert += ` ON CONFLICT (identifier, account_id)
                       DO UPDATE SET
                        name = EXCLUDED.name,
                        phone_number = EXCLUDED.phone_number,
                        identifier = EXCLUDED.identifier`;

        totalContactsImported += (await pgClient.query(sqlInsert, bindInsert))?.rowCount ?? 0;
        contactsChunk = this.sliceIntoChunks(contacts, 3000);
      }

      await this.insertTaggings(tagId, contacts, createdAt);

      this.deleteHistoryContacts(instance);

      return totalContactsImported;
    } catch (error) {
      this.logger.error(`Error on import history contacts: ${error.toString()}`);
    }
  }

  public async importHistoryMessages(
    instance: InstanceDto,
    chatwootService: ChatwootService,
    inbox: inbox,
    provider: ChatwootRaw,
  ) {
    try {
      const pgClient = postgresClient.getChatwootConnection();

      const chatwootUser = await this.getChatwootUser(provider);
      if (!chatwootUser) {
        throw new Error('User not found to import messages.');
      }

      let totalMessagesImported = 0;

      const messagesOrdered = this.historyMessages.get(instance.instanceName) || [];
      if (messagesOrdered.length === 0) {
        return 0;
      }

      messagesOrdered.sort((a, b) => {
        return (
          parseInt(a.key.remoteJid) - parseInt(b.key.remoteJid) ||
          (a.messageTimestamp as number) - (b.messageTimestamp as number)
        );
      });

      const allMessagesMappedByPhoneNumber = this.createMessagesMapByPhoneNumber(messagesOrdered);
      const phoneNumbersWithTimestamp = new Map<string, firstLastTimestamp>();
      allMessagesMappedByPhoneNumber.forEach((messages: MessageRaw[], phoneNumber: string) => {
        phoneNumbersWithTimestamp.set(phoneNumber, {
          first: messages[0]?.messageTimestamp as number,
          last: messages[messages.length - 1]?.messageTimestamp as number,
        });
      });

      const batchSize = 4000;
      let messagesChunk: MessageRaw[] = this.sliceIntoChunks(messagesOrdered, batchSize);
      while (messagesChunk.length > 0) {
        const messagesByPhoneNumber = this.createMessagesMapByPhoneNumber(messagesChunk);

        if (messagesByPhoneNumber.size > 0) {
          const fksByNumber = await this.selectOrCreateFksFromChatwoot(
            provider,
            inbox,
            phoneNumbersWithTimestamp,
            messagesByPhoneNumber,
          );

          let sqlInsertMsg = `INSERT INTO messages
            (content, account_id, inbox_id, conversation_id, message_type, private, status, created_at, updated_at) VALUES `;

          let bindInsertMsg = [];
          for (const [phoneNumber, messages] of messagesByPhoneNumber) {
            for (const message of messages) {
              const fks: FksChatwoot = fksByNumber.get(phoneNumber);
              const bindAccountId = `$${bindInsertMsg.push(provider.account_id)}`;
              const bindInboxId = `$${bindInsertMsg.push(provider.id)}`;
              const bindConversationId = `$${bindInsertMsg.push(fks.conversation_id)}`;
              const bindMessage = `$${bindInsertMsg.push(message.messageText || '')}`;
              const bindMessageType = `$${bindInsertMsg.push('incoming')}`;
              const bindMessagePrivate = `$${bindInsertMsg.push(false)}`;
              const bindMessageStatus = `$${bindInsertMsg.push('sent')}`;

              sqlInsertMsg += `(${bindMessage}, ${bindAccountId}, ${bindInboxId}, ${bindConversationId}, ${bindMessageType}, ${bindMessagePrivate}, ${bindMessageStatus}, NOW(), NOW()),`;
            }
          }

          if (sqlInsertMsg.slice(-1) === ',') {
            sqlInsertMsg = sqlInsertMsg.slice(0, -1);
          }
          totalMessagesImported += (await pgClient.query(sqlInsertMsg, bindInsertMsg))?.rowCount ?? 0;
        }
        messagesChunk = this.sliceIntoChunks(messagesOrdered, batchSize);
      }

      this.deleteHistoryMessages(instance);
      this.clearAll(instance);

      return totalMessagesImported;
    } catch (error) {
      this.logger.error(`Error on import history messages: ${error.toString()}`);
    }
  }

  private async getChatwootUser(provider: ChatwootRaw): Promise<ChatwootUser | null> {
    try {
      const pgClient = postgresClient.getChatwootConnection();
      const sqlSelectChatwootUser = `SELECT user_type, user_id FROM users WHERE email = $1`;
      const user = await pgClient.query(sqlSelectChatwootUser, [provider.email]);
      return user?.rows?.[0] ?? null;
    } catch (error) {
      this.logger.error(`Error on select chatwoot user: ${error.toString()}`);
      return null;
    }
  }

  private async selectOrCreateFksFromChatwoot(
    provider: ChatwootRaw,
    inbox: inbox,
    phoneNumbersWithTimestamp: Map<string, firstLastTimestamp>,
    messagesByPhoneNumber: Map<string, MessageRaw[]>,
  ) {
    const fksByNumber = new Map<string, FksChatwoot>();

    try {
      const pgClient = postgresClient.getChatwootConnection();
      const selectOrCreateContactConversation = `
        WITH ins1 AS (
          INSERT INTO contacts
            (account_id, phone_number, name, created_at, updated_at, identifier)
          VALUES
            ($1, $2, $3, to_timestamp($4), to_timestamp($4), $5)
          ON CONFLICT (identifier, account_id) DO NOTHING
          RETURNING id AS contact_id
        ),
        ins2 AS (
          INSERT INTO conversations
            (account_id, inbox_id, contact_id, created_at, updated_at)
          VALUES
            ($1, $6, COALESCE((SELECT contact_id FROM ins1), (SELECT id FROM contacts WHERE identifier = $5 AND account_id = $1)), to_timestamp($7), to_timestamp($7))
          RETURNING id AS conversation_id, contact_id
        )
        SELECT
          COALESCE((SELECT contact_id FROM ins1), (SELECT id FROM contacts WHERE identifier = $5 AND account_id = $1)) AS contact_id,
          COALESCE((SELECT conversation_id FROM ins2), (SELECT id FROM conversations WHERE contact_id = (COALESCE((SELECT contact_id FROM ins1), (SELECT id FROM contacts WHERE identifier = $5 AND account_id = $1))) AND account_id = $1)) AS conversation_id
      `;

      for (const [phoneNumber, messages] of messagesByPhoneNumber) {
        const firstLastTimestamp = phoneNumbersWithTimestamp.get(phoneNumber);

        const contactName = messages[0].pushName || '';
        const bindContactAccountId = provider.account_id;
        const bindContactPhoneNumber = `+${phoneNumber.split('@')[0]}`;
        const bindContactName = contactName;
        const bindContactCreatedAt = firstLastTimestamp.first;
        const bindContactIdentifier = `${phoneNumber}`;
        const bindConversationInboxId = provider.id;
        const bindConversationCreatedAt = firstLastTimestamp.first;

        const selectContactConversationResult = await pgClient.query(selectOrCreateContactConversation, [
          bindContactAccountId,
          bindContactPhoneNumber,
          bindContactName,
          bindContactCreatedAt,
          bindContactIdentifier,
          bindConversationInboxId,
          bindConversationCreatedAt,
        ]);

        const contact_id = selectContactConversationResult?.rows?.[0]?.contact_id;
        const conversation_id = selectContactConversationResult?.rows?.[0]?.conversation_id;

        fksByNumber.set(phoneNumber, {
          phone_number: phoneNumber,
          contact_id,
          conversation_id,
        });
      }
    } catch (error) {
      this.logger.error(`Error on select or create Fks from chatwoot: ${error.toString()}`);
    }
    return fksByNumber;
  }

  private createMessagesMapByPhoneNumber(messages: MessageRaw[]) {
    return messages.reduce((map: Map<string, MessageRaw[]>, message: MessageRaw) => {
      const phoneNumber = message.key.remoteJid;
      const messagesList = map.get(phoneNumber) || [];
      messagesList.push(message);
      map.set(phoneNumber, messagesList);
      return map;
    }, new Map<string, MessageRaw[]>());
  }

  private sliceIntoChunks(array: any[], chunkSize: number) {
    return array.slice(0, chunkSize);
  }
}

export { ChatwootImport };
