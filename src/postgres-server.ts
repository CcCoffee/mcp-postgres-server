#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { Client } from 'pg';

interface PostgresConfig {
  url: string;
  username: string;
  password: string;
}

interface ToolResponse {
  content: { type: string; text: string }[];
  isError?: boolean;
  _meta?: any;
}

class PostgresServer {
  private server: Server;

  constructor() {
    this.server = new Server(
      {
        name: 'postgres-server',
        version: '0.1.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.setupToolHandlers();
    
    this.server.onerror = (error) => console.error('[MCP Error]', error);
    process.on('SIGINT', async () => {
      await this.server.close();
      process.exit(0);
    });
  }

  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'test_connection',
          description: '测试数据库连接',
          inputSchema: {
            type: 'object',
            properties: {
              url: { type: 'string', description: '数据库连接URL' },
              username: { type: 'string', description: '用户名' },
              password: { type: 'string', description: '密码' }
            },
            required: ['url', 'username', 'password']
          }
        },
        {
          name: 'execute_query',
          description: '执行SQL查询',
          inputSchema: {
            type: 'object',
            properties: {
              url: { type: 'string', description: '数据库连接URL' },
              username: { type: 'string', description: '用户名' },
              password: { type: 'string', description: '密码' },
              query: { type: 'string', description: 'SQL查询语句' }
            },
            required: ['url', 'username', 'password', 'query']
          }
        },
        {
          name: 'create_table',
          description: '创建数据库表',
          inputSchema: {
            type: 'object',
            properties: {
              url: { type: 'string', description: '数据库连接URL' },
              username: { type: 'string', description: '用户名' },
              password: { type: 'string', description: '密码' },
              tableName: { type: 'string', description: '表名' },
              columns: { 
                type: 'array', 
                description: '列定义',
                items: {
                  type: 'object',
                  properties: {
                    name: { type: 'string', description: '列名' },
                    type: { type: 'string', description: '列类型' }
                  },
                  required: ['name', 'type']
                }
              }
            },
            required: ['url', 'username', 'password', 'tableName', 'columns']
          }
        },
        {
          name: 'insert_data',
          description: '向表中插入数据',
          inputSchema: {
            type: 'object',
            properties: {
              url: { type: 'string', description: '数据库连接URL' },
              username: { type: 'string', description: '用户名' },
              password: { type: 'string', description: '密码' },
              tableName: { type: 'string', description: '表名' },
              data: { 
                type: 'array', 
                description: '要插入的数据',
                items: { type: 'object' }
              }
            },
            required: ['url', 'username', 'password', 'tableName', 'data']
          }
        }
      ]
    }));

    this.server.setRequestHandler(
      CallToolRequestSchema, 
      async (request: any) => {
        switch (request.params.name) {
          case 'test_connection':
            return this.testConnection(request.params.arguments);
          case 'execute_query':
            return this.executeQuery(request.params.arguments);
          case 'create_table':
            return this.createTable(request.params.arguments);
          case 'insert_data':
            return this.insertData(request.params.arguments);
          default:
            throw new McpError(ErrorCode.MethodNotFound, `未知工具: ${request.params.name}`);
        }
      }
    );
  }

  private async testConnection(args: any): Promise<ToolResponse> {
    const { url, username, password } = args;
    const client = new Client({ connectionString: url, user: username, password });
    
    try {
      await client.connect();
      await client.end();
      return {
        content: [{ 
          type: 'text', 
          text: '数据库连接成功' 
        }],
        _meta: {}
      };
    } catch (error) {
      return {
        content: [{ 
          type: 'text', 
          text: `数据库连接失败: ${error instanceof Error ? error.message : '未知错误'}` 
        }],
        isError: true,
        _meta: {}
      };
    }
  }

  private async executeQuery(args: any): Promise<ToolResponse> {
    const { url, username, password, query } = args;
    const client = new Client({ connectionString: url, user: username, password });
    
    try {
      await client.connect();
      const result = await client.query(query);
      await client.end();
      return {
        content: [{ 
          type: 'text', 
          text: JSON.stringify(result.rows, null, 2) 
        }],
        _meta: {}
      };
    } catch (error) {
      return {
        content: [{ 
          type: 'text', 
          text: `查询执行失败: ${error instanceof Error ? error.message : '未知错误'}` 
        }],
        isError: true,
        _meta: {}
      };
    }
  }

  private async createTable(args: any): Promise<ToolResponse> {
    const { url, username, password, tableName, columns } = args;
    const client = new Client({ connectionString: url, user: username, password });
    
    try {
      await client.connect();
      
      // 构建创建表的SQL语句
      const columnDefinitions = columns.map(col => `${col.name} ${col.type}`).join(', ');
      const createTableQuery = `CREATE TABLE ${tableName} (${columnDefinitions})`;
      
      await client.query(createTableQuery);
      await client.end();
      
      return {
        content: [{ 
          type: 'text', 
          text: `表 ${tableName} 创建成功` 
        }],
        _meta: {}
      };
    } catch (error) {
      return {
        content: [{ 
          type: 'text', 
          text: `创建表失败: ${error instanceof Error ? error.message : '未知错误'}` 
        }],
        isError: true,
        _meta: {}
      };
    }
  }

  private async insertData(args: any): Promise<ToolResponse> {
    const { url, username, password, tableName, data } = args;
    const client = new Client({ connectionString: url, user: username, password });
    
    try {
      await client.connect();
      
      // 构建插入数据的SQL语句
      for (const record of data) {
        const columns = Object.keys(record).join(', ');
        const values = Object.values(record).map(val => 
          typeof val === 'string' ? `'${val}'` : val
        ).join(', ');
        
        const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
        await client.query(insertQuery);
      }
      
      await client.end();
      
      return {
        content: [{ 
          type: 'text', 
          text: `成功向 ${tableName} 插入 ${data.length} 条数据` 
        }],
        _meta: {}
      };
    } catch (error) {
      return {
        content: [{ 
          type: 'text', 
          text: `插入数据失败: ${error instanceof Error ? error.message : '未知错误'}` 
        }],
        isError: true,
        _meta: {}
      };
    }
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('Postgres MCP服务器正在运行');
  }
}

const server = new PostgresServer();
server.run().catch(console.error);
