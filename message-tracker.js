/**
 * 消息处理状态跟踪器
 * 使用 Cloudflare KV 存储已处理的消息ID，避免重复处理
 */

export class MessageTracker {
    constructor(kv, options = {}) {
        this.kv = kv;
        this.expirationTtl = options.expirationTtl || 86400; // 24小时过期
        this.keyPrefix = options.keyPrefix || 'processed_msg';
    }

    /**
     * 生成消息处理状态的 KV 键
     */
    getMessageKey(msgId) {
        return `${this.keyPrefix}:${msgId}`;
    }

    /**
     * 检查消息是否已处理
     * @param {string} msgId 消息ID
     * @returns {Promise<boolean>} 如果消息已处理返回true，否则返回false
     */
    async isMessageProcessed(msgId) {
        try {
            if (!msgId) {
                console.warn('消息ID为空，视为未处理');
                return false;
            }

            const key = this.getMessageKey(msgId);
            const result = await this.kv.get(key, { type: 'json' });
            
            // 如果消息不存在，返回未处理
            if (!result) {
                return false;
            }
            
            // 如果消息已处理，但超过5分钟，视为新消息（允许重试）
            const fiveMinutesAgo = Date.now() - (5 * 60 * 1000);
            if (result.timestamp && result.timestamp < fiveMinutesAgo) {
                console.log(`消息 ${msgId} 已过期，重新处理`, {
                    lastProcessed: new Date(result.timestamp).toISOString(),
                    now: new Date().toISOString()
                });
                return false;
            }
            
            console.log(`消息 ${msgId} 已处理，跳过`, {
                processedAt: result.processedAt
            });
            return true;
        } catch (error) {
            console.error('检查消息处理状态失败:', {
                error: error.message,
                msgId,
                stack: error.stack
            });
            // 出错时返回未处理，确保消息不会被漏掉
            return false;
        }
    }

    /**
     * 标记消息为已处理
     * @param {string} msgId 消息ID
     * @param {Object} [metadata={}] 元数据
     * @param {string} [metadata.externalUserid] 外部用户ID
     * @param {string} [metadata.content] 消息内容
     * @param {string} [metadata.assistantMessage] 助手回复
     * @param {boolean} [metadata.success] 是否处理成功
     * @param {string} [metadata.error] 错误信息
     * @returns {Promise<boolean>} 是否标记成功
     */
    async markMessageAsProcessed(msgId, metadata = {}) {
        if (!msgId) {
            console.error('无法标记空消息ID为已处理');
            return false;
        }

        try {
            const key = this.getMessageKey(msgId);
            const data = {
                msgId,
                processedAt: new Date().toISOString(),
                timestamp: Date.now(),
                ...metadata,
            };

            await this.kv.put(
                key,
                JSON.stringify(data),
                { expirationTtl: this.expirationTtl }
            );

            console.log(`消息 ${msgId} 已标记为已处理`, {
                key,
                externalUserid: metadata.externalUserid,
                success: metadata.success,
                timestamp: data.timestamp
            });

            return true;
        } catch (error) {
            console.error('标记消息处理状态失败:', {
                error: error.message,
                msgId,
                metadata,
                stack: error.stack
            });
            return false;
        }
    }

    /**
     * 获取消息处理信息
     */
    async getMessageProcessInfo(msgId) {
        try {
            const key = this.getMessageKey(msgId);
            const result = await this.kv.get(key);

            if (!result) {
                return null;
            }

            return JSON.parse(result);
        } catch (error) {
            console.error('获取消息处理信息失败:', error);
            return null;
        }
    }

    /**
     * 批量检查消息处理状态
     */
    async checkMultipleMessages(msgIds) {
        const results = {};

        try {
            // 并发检查多个消息状态
            const promises = msgIds.map(async msgId => {
                const processed = await this.isMessageProcessed(msgId);
                return { msgId, processed };
            });

            const checkResults = await Promise.all(promises);

            checkResults.forEach(({ msgId, processed }) => {
                results[msgId] = processed;
            });

            return results;
        } catch (error) {
            console.error('批量检查消息状态失败:', error);
            // 返回所有消息都未处理的结果（安全策略）
            msgIds.forEach(msgId => {
                results[msgId] = false;
            });
            return results;
        }
    }

    /**
     * 删除消息处理记录
     */
    async removeMessageRecord(msgId) {
        try {
            const key = this.getMessageKey(msgId);
            await this.kv.delete(key);
            console.log(`消息 ${msgId} 的处理记录已删除`);
        } catch (error) {
            console.error('删除消息处理记录失败:', error);
            throw error;
        }
    }

    /**
     * 清理过期的消息处理记录
     * 注意：KV 会自动处理过期，这个方法主要用于手动清理
     */
    async cleanupExpiredRecords() {
        try {
            // 由于 KV 的限制，我们无法直接列出所有键
            // KV 会自动清理过期的记录
            console.log('KV 会自动清理过期的消息处理记录');
        } catch (error) {
            console.error('清理过期记录失败:', error);
        }
    }

    /**
     * 获取处理统计信息
     * 由于 KV 限制，只能提供基本信息
     */
    getProcessingStats() {
        return {
            keyPrefix: this.keyPrefix,
            expirationTtl: this.expirationTtl,
            autoExpiry: true,
            note: 'KV存储会自动清理过期记录',
        };
    }

    /**
     * 清除消息处理状态
     * @param {string} msgId 消息ID
     * @returns {Promise<boolean>} 是否清除成功
     */
    async clearMessageStatus(msgId) {
        try {
            const key = this.getMessageKey(msgId);
            await this.kv.delete(key);
            console.log(`已清除消息 ${msgId} 的处理状态`);
            return true;
        } catch (error) {
            console.error('清除消息处理状态失败:', {
                error: error.message,
                msgId,
                stack: error.stack
            });
            return false;
        }
    }

    /**
     * 批量检查消息处理状态
     * @param {string[]} msgIds 消息ID数组
     * @returns {Promise<Object>} 消息ID到处理状态的映射
     */
    async checkMultipleMessages(msgIds) {
        const results = {};

        try {
            // 并发检查多个消息状态
            const promises = msgIds.map(async msgId => {
                const processed = await this.isMessageProcessed(msgId);
                return { msgId, processed };
            });

            const checkResults = await Promise.all(promises);

            // 转换为对象格式
            checkResults.forEach(({ msgId, processed }) => {
                results[msgId] = processed;
            });

            return results;
        } catch (error) {
            console.error('批量检查消息状态失败:', error);
            // 返回所有消息都未处理的结果（安全策略）
            return msgIds.reduce((acc, msgId) => {
                acc[msgId] = false;
                return acc;
            }, {});
        }
    }
}
