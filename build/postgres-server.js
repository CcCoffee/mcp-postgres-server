#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { CallToolRequestSchema, ListToolsRequestSchema, ListResourcesRequestSchema, ReadResourceRequestSchema, } from '@modelcontextprotocol/sdk/types.js';
import pkg from 'pg';
const { Client, Pool } = pkg;
class PostgresServer {
    constructor() {
        const { POSTGRES_URL, POSTGRES_USERNAME, POSTGRES_PASSWORD } = process.env;
        if (!POSTGRES_URL || !POSTGRES_USERNAME || !POSTGRES_PASSWORD) {
            throw new Error('缺少必要的数据库连接环境变量');
        }
        this.connectionString = `postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@${POSTGRES_URL}`;
        this.pool = new Pool({ connectionString: this.connectionString });
        this.server = new Server({
            name: 'postgres-server',
            version: '0.1.0',
        }, {
            capabilities: {
                resources: {},
                tools: {},
            },
        });
        this.setupHandlers();
    }
    setupHandlers() {
        // 资源处理程序
        this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
            const client = await this.pool.connect();
            try {
                const result = await client.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
                return {
                    resources: result.rows.map((row) => ({
                        uri: `postgres://${row.table_name}/schema`,
                        mimeType: 'application/json',
                        name: `"${row.table_name}" 数据库架构`,
                    })),
                };
            }
            finally {
                client.release();
            }
        });
        this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
            const client = await this.pool.connect();
            try {
                const tableName = request.params.uri.split('/')[2];
                const result = await client.query('SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1', [tableName]);
                return {
                    contents: [{
                            uri: request.params.uri,
                            mimeType: 'application/json',
                            text: JSON.stringify(result.rows, null, 2),
                        }],
                };
            }
            finally {
                client.release();
            }
        });
        // 工具处理程序
        this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
            tools: [
                {
                    name: 'query',
                    description: '执行只读SQL查询',
                    inputSchema: {
                        type: 'object',
                        properties: {
                            sql: {
                                type: 'string',
                                description: 'SQL查询语句'
                            }
                        },
                        required: ['sql']
                    }
                },
                {
                    name: 'test_connection',
                    description: '测试数据库连接',
                    inputSchema: {
                        type: 'object',
                        properties: {}
                    }
                },
                {
                    name: 'create_table',
                    description: '创建数据库表',
                    inputSchema: {
                        type: 'object',
                        properties: {
                            tableName: {
                                type: 'string',
                                description: '表名'
                            },
                            columns: {
                                type: 'array',
                                description: '列定义',
                                items: {
                                    type: 'object',
                                    properties: {
                                        name: {
                                            type: 'string',
                                            description: '列名'
                                        },
                                        type: {
                                            type: 'string',
                                            description: '列类型'
                                        }
                                    },
                                    required: ['name', 'type']
                                }
                            }
                        },
                        required: ['tableName', 'columns']
                    }
                }
            ]
        }));
        // 工具调用处理程序
        this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
            switch (request.params.name) {
                case 'query':
                    return this.executeQuery(request.params.arguments);
                case 'test_connection':
                    return this.testConnection();
                case 'create_table':
                    return this.createTable(request.params.arguments);
                default:
                    throw new Error(`未知工具: ${request.params.name}`);
            }
        });
    }
    async testConnection() {
        const client = new Client({ connectionString: this.connectionString });
        try {
            await client.connect();
            await client.end();
            return {
                content: [{
                        type: 'text',
                        text: '数据库连接成功'
                    }]
            };
        }
        catch (error) {
            return {
                content: [{
                        type: 'text',
                        text: `数据库连接失败: ${error instanceof Error ? error.message : '未知错误'}`
                    }],
                isError: true
            };
        }
    }
    async executeQuery(args) {
        const { sql } = args;
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN TRANSACTION READ ONLY');
            const result = await client.query(sql);
            return {
                content: [{
                        type: 'text',
                        text: JSON.stringify(result.rows, null, 2)
                    }]
            };
        }
        catch (error) {
            return {
                content: [{
                        type: 'text',
                        text: `查询执行失败: ${error instanceof Error ? error.message : '未知错误'}`
                    }],
                isError: true
            };
        }
        finally {
            await client.query('ROLLBACK').catch(console.warn);
            client.release();
        }
    }
    async createTable(args) {
        const { tableName, columns } = args;
        const client = await this.pool.connect();
        try {
            const columnDefinitions = columns.map(col => `${col.name} ${col.type}`).join(', ');
            const createTableQuery = `CREATE TABLE ${tableName} (${columnDefinitions})`;
            await client.query(createTableQuery);
            return {
                content: [{
                        type: 'text',
                        text: `表 ${tableName} 创建成功`
                    }]
            };
        }
        catch (error) {
            return {
                content: [{
                        type: 'text',
                        text: `创建表失败: ${error instanceof Error ? error.message : '未知错误'}`
                    }],
                isError: true
            };
        }
        finally {
            client.release();
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
