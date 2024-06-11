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
      while (contactsChunk.length > 0) {
        // inserting contacts in chatwoot db
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

          // Inserindo o label para cada contato
          await this.insertLabel(instance.instanceName, Number(provider.account_id));
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

      // ordering messages by number and timestamp asc
      messagesOrdered.sort((a, b) => {
        return (
          parseInt(a.key.remoteJid) - parseInt(b.key.remoteJid) ||
          (a.messageTimestamp as number) - (b.messageTimestamp as number)
        );
      });

      const allMessagesMappedByPhoneNumber = this.createMessagesMapByPhoneNumber(messagesOrdered);
      // Map structure: +552199999999 => { first message timestamp from number, last message timestamp from number}
      const phoneNumbersWithTimestamp = new Map<string, firstLastTimestamp>();
      allMessagesMappedByPhoneNumber.forEach((messages: MessageRaw[], phoneNumber: string) => {
        phoneNumbersWithTimestamp.set(phoneNumber, {
          first: messages[0]?.messageTimestamp as number,
          last: messages[messages.length - 1]?.messageTimestamp as number,
        });
      });

      // processing messages in batch
      const batchSize = 4000;
      let messagesChunk: MessageRaw[] = this.sliceIntoChunks(messagesOrdered, batchSize);
      while (messagesChunk.length > 0) {
        // Map structure: +552199999999 => MessageRaw[]
        const messagesByPhoneNumber = this.createMessagesMapByPhoneNumber(messagesChunk);

        if (messagesByPhoneNumber.size > 0) {
          const fksByNumber = await this.selectOrCreateFksFromChatwoot(
            provider,
            inbox,
            phoneNumbersWithTimestamp,
            messagesByPhoneNumber,
          );

          // inserting messages in chatwoot db
          let sqlInsertMsg = `INSERT INTO messages
            (content, account_id, inbox_id, conversation_id, message_type, private, content_type,
            sender_type, sender_id, created_at, updated_at) VALUES `;
          const bindInsertMsg = [provider.account_id, inbox.id];

          messagesByPhoneNumber.forEach((messages: MessageRaw[], phoneNumber: string) => {
            const fksChatwoot = fksByNumber.get(phoneNumber);

            messages.forEach((message) => {
              if (!message.message) {
                return;
              }

              if (!fksChatwoot?.conversation_id || !fksChatwoot?.contact_id) {
                return;
              }

              const contentMessage = this.getContentMessage(chatwootService, message);
              if (!contentMessage) {
                return;
              }

              bindInsertMsg.push(contentMessage);
              const bindContent = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(fksChatwoot.conversation_id);
              const bindConversationId = `$${bindInsertMsg.length}`;

              bindInsertMsg.push(message.messageTimestamp);
              const bindCreatedAt = `to_timestamp($${bindInsertMsg.length}::double precision)`;

              sqlInsertMsg += `(${bindContent}, $1, $2, ${bindConversationId}, 'incoming', FALSE, 'text',
              'Contact', ${fksChatwoot.contact_id}, ${bindCreatedAt}, ${bindCreatedAt}),`;
            });
          });
          if (sqlInsertMsg.slice(-1) === ',') {
            sqlInsertMsg = sqlInsertMsg.slice(0, -1);
          }
          sqlInsertMsg += ` ON CONFLICT (conversation_id, created_at)
                            DO UPDATE SET
                              content = EXCLUDED.content`;

          totalMessagesImported += (await pgClient.query(sqlInsertMsg, bindInsertMsg))?.rowCount ?? 0;
          messagesChunk = this.sliceIntoChunks(messagesOrdered, batchSize);
        }
      }

      this.deleteHistoryMessages(instance);

      return totalMessagesImported;
    } catch (error) {
      this.logger.error(`Error on import history messages: ${error.toString()}`);
    }
  }

  private createMessagesMapByPhoneNumber(messages: MessageRaw[]): Map<string, MessageRaw[]> {
    const messagesByPhoneNumber = new Map<string, MessageRaw[]>();
    messages.forEach((message) => {
      const phoneNumber = `+${message.key.remoteJid.split('@')[0]}`;
      const previousMessages = messagesByPhoneNumber.get(phoneNumber) || [];
      previousMessages.push(message);
      messagesByPhoneNumber.set(phoneNumber, previousMessages);
    });
    return messagesByPhoneNumber;
  }

  private sliceIntoChunks(messages: MessageRaw[], chunkSize: number): MessageRaw[] {
    return messages.splice(0, chunkSize);
  }

  private async selectOrCreateFksFromChatwoot(
    provider: ChatwootRaw,
    inbox: inbox,
    phoneNumbersWithTimestamp: Map<string, firstLastTimestamp>,
    messagesByPhoneNumber: Map<string, MessageRaw[]>,
  ): Promise<Map<string, FksChatwoot>> {
    const fksByNumber = new Map<string, FksChatwoot>();

    const phoneNumbers = [...messagesByPhoneNumber.keys()];

    const pgClient = postgresClient.getChatwootConnection();

    const sql = `
      SELECT c.phone_number, c.id as contact_id, con.id as conversation_id
      FROM contacts c
      LEFT JOIN conversations con
      ON con.contact_id = c.id
      WHERE c.phone_number = ANY ($1::text[])
    `;
    const rows = (await pgClient.query(sql, [phoneNumbers]))?.rows || [];

    for (const phoneNumber of phoneNumbers) {
      const contact = rows.find((row: any) => row.phone_number === phoneNumber);

      if (contact?.conversation_id) {
        fksByNumber.set(phoneNumber, contact);
        continue;
      }

      const messages = messagesByPhoneNumber.get(phoneNumber) || [];
      if (messages.length === 0) {
        continue;
      }

      const firstMessageTimestamp = phoneNumbersWithTimestamp.get(phoneNumber)?.first;
      const firstMessage = messages.find((message) => message.messageTimestamp === firstMessageTimestamp);

      const identifier = firstMessage?.key.remoteJid;

      if (!identifier) {
        continue;
      }

      const contact_id = await this.insertContactChatwoot(provider, phoneNumber, identifier);
      const conversation_id = await this.insertConversationChatwoot(provider, inbox, contact_id);

      fksByNumber.set(phoneNumber, { phone_number: phoneNumber, contact_id, conversation_id });
    }

    return fksByNumber;
  }

  private async insertContactChatwoot(provider: ChatwootRaw, phone_number: string, identifier: string) {
    const pgClient = postgresClient.getChatwootConnection();
    const sqlInsert = `
      INSERT INTO contacts
        (name, phone_number, account_id, identifier, created_at, updated_at)
      VALUES
        ($1, $2, $3, $4, NOW(), NOW())
      RETURNING id
    `;
    const bindInsert = [null, phone_number, provider.account_id, identifier];
    const result = await pgClient.query(sqlInsert, bindInsert);
    return result?.rows?.[0]?.id;
  }

  private async insertConversationChatwoot(provider: ChatwootRaw, inbox: inbox, contact_id: string) {
    const pgClient = postgresClient.getChatwootConnection();
    const sqlInsert = `
      INSERT INTO conversations
        (account_id, inbox_id, contact_id, status, created_at, updated_at)
      VALUES
        ($1, $2, $3, 'open', NOW(), NOW())
      RETURNING id
    `;
    const bindInsert = [provider.account_id, inbox.id, contact_id];
    const result = await pgClient.query(sqlInsert, bindInsert);
    return result?.rows?.[0]?.id;
  }

  private async getChatwootUser(provider: ChatwootRaw): Promise<ChatwootUser | null> {
    const pgClient = postgresClient.getChatwootConnection();
    const sql = `
      SELECT u.id as user_id, u.user_type
      FROM users u
      WHERE u.email = $1
    `;
    const result = await pgClient.query(sql, [provider.email]);
    return result?.rows?.[0] || null;
  }

  private getContentMessage(chatwootService: ChatwootService, message: MessageRaw) {
    if (message?.message?.conversation) {
      return message.message.conversation;
    }
    return chatwootService.getMessageContent(message.message);
  }
}
