/*
 * Share consumer (KIP-932) probe against the docker/kafka-share bench.
 *
 * Not part of the component. Verifies the librdkafka C API behaviour the
 * component relies on (specs/share-consumer.md, step 3):
 *   1. which regular-consumer properties rd_kafka_share_consumer_new() accepts;
 *   2. explicit acknowledgement: ACCEPT / RELEASE / REJECT and redelivery;
 *   3. poll with unacknowledged records returns __STATE;
 *   4. setting share.auto.offset.reset on a group via IncrementalAlterConfigs.
 *
 * Usage: share_probe [bootstrap]   (default localhost:9092)
 * Exit code 0 when every check passes.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <librdkafka/rdkafka.h>

static const char *brokers = "localhost:9092";
static int failures        = 0;

#define CHECK(cond, ...)                                                       \
        do {                                                                   \
                if (cond) {                                                    \
                        printf("  OK   ");                                     \
                } else {                                                       \
                        printf("  FAIL ");                                     \
                        failures++;                                            \
                }                                                              \
                printf(__VA_ARGS__);                                           \
                printf("\n");                                                  \
        } while (0)

static rd_kafka_conf_t *base_conf(void) {
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "bootstrap.servers: %s\n", errstr);
                exit(2);
        }
        return conf;
}

/* Produce n messages "msg-<i>" to topic, wait for delivery. */
static void produce(const char *topic, int n) {
        char errstr[512];
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, base_conf(), errstr,
                                      sizeof(errstr));
        int i;
        if (!rk) {
                fprintf(stderr, "producer: %s\n", errstr);
                exit(2);
        }
        for (i = 0; i < n; i++) {
                char payload[32];
                snprintf(payload, sizeof(payload), "msg-%d", i);
                rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic),
                                  RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                  RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                  RD_KAFKA_V_END);
        }
        CHECK(rd_kafka_flush(rk, 10000) == RD_KAFKA_RESP_ERR_NO_ERROR,
              "produced %d messages to %s", n, topic);
        rd_kafka_destroy(rk);
}

/* Set share.auto.offset.reset=earliest on a group (broker-side group config). */
static void set_group_earliest(const char *group) {
        char errstr[512];
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, base_conf(), errstr,
                                      sizeof(errstr));
        rd_kafka_queue_t *q = rd_kafka_queue_new(rk);
        rd_kafka_ConfigResource_t *res =
            rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_GROUP, group);
        rd_kafka_error_t *error = rd_kafka_ConfigResource_add_incremental_config(
            res, "share.auto.offset.reset", RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET,
            "earliest");
        rd_kafka_event_t *ev;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__TIMED_OUT;
        const char *errmsg      = "timeout";

        if (error) {
                CHECK(0, "add_incremental_config: %s",
                      rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                return;
        }
        rd_kafka_IncrementalAlterConfigs(rk, &res, 1, NULL, q);
        ev = rd_kafka_queue_poll(q, 15000);
        if (ev) {
                const rd_kafka_IncrementalAlterConfigs_result_t *result =
                    rd_kafka_event_IncrementalAlterConfigs_result(ev);
                err    = rd_kafka_event_error(ev);
                errmsg = rd_kafka_event_error_string(ev);
                if (!err && result) {
                        size_t cnt;
                        const rd_kafka_ConfigResource_t **rres =
                            rd_kafka_IncrementalAlterConfigs_result_resources(
                                result, &cnt);
                        if (cnt > 0) {
                                err    = rd_kafka_ConfigResource_error(rres[0]);
                                errmsg = rd_kafka_ConfigResource_error_string(
                                    rres[0]);
                        }
                }
        }
        CHECK(err == RD_KAFKA_RESP_ERR_NO_ERROR,
              "IncrementalAlterConfigs GROUP %s share.auto.offset.reset=earliest"
              " (%s)",
              group, err ? (errmsg ? errmsg : rd_kafka_err2str(err)) : "ok");
        if (ev)
                rd_kafka_event_destroy(ev);
        rd_kafka_ConfigResource_destroy(res);
        rd_kafka_queue_destroy(q);
        rd_kafka_destroy(rk);
}

/* Check 1: regular-consumer properties on a share consumer conf. */
static void check_properties(const char *group) {
        static const char *props[][2] = {
            {"enable.auto.commit", "false"},
            {"auto.offset.reset", "earliest"},
            {"isolation.level", "read_committed"},
            {"enable.partition.eof", "true"},
            {"partition.assignment.strategy", "roundrobin"},
            {"session.timeout.ms", "45000"},
            {"max.poll.interval.ms", "300000"},
            {"group.protocol", "consumer"},
            {"max.poll.records", "100"},
            {"statistics.interval.ms", "60000"},
        };
        size_t i;

        printf("[1] regular-consumer properties on a share consumer\n");
        for (i = 0; i < sizeof(props) / sizeof(props[0]); i++) {
                char errstr[512];
                rd_kafka_conf_t *conf = base_conf();
                rd_kafka_share_t *rks;
                rd_kafka_conf_res_t r;

                rd_kafka_conf_set(conf, "group.id", group, errstr,
                                  sizeof(errstr));
                r = rd_kafka_conf_set(conf, props[i][0], props[i][1], errstr,
                                      sizeof(errstr));
                if (r != RD_KAFKA_CONF_OK) {
                        printf("  INFO %s=%s rejected by conf_set: %s\n",
                               props[i][0], props[i][1], errstr);
                        rd_kafka_conf_destroy(conf);
                        continue;
                }
                errstr[0] = '\0';
                rks = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
                if (!rks) {
                        printf("  INFO %s=%s rejected by share_consumer_new: %s\n",
                               props[i][0], props[i][1], errstr);
                        rd_kafka_conf_destroy(conf);
                        continue;
                }
                printf("  INFO %s=%s accepted\n", props[i][0], props[i][1]);
                rd_kafka_share_consumer_close(rks);
                rd_kafka_share_destroy(rks);
        }
}

static rd_kafka_share_t *new_share_consumer(const char *group,
                                            const char *topic) {
        char errstr[512];
        rd_kafka_conf_t *conf = base_conf();
        rd_kafka_share_t *rks;
        rd_kafka_topic_partition_list_t *topics;
        rd_kafka_resp_err_t err;

        rd_kafka_conf_set(conf, "group.id", group, errstr, sizeof(errstr));
        if (rd_kafka_conf_set(conf, "share.acknowledgement.mode", "explicit",
                              errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "share.acknowledgement.mode: %s\n", errstr);
                exit(2);
        }
        rks = rd_kafka_share_consumer_new(conf, errstr, sizeof(errstr));
        if (!rks) {
                fprintf(stderr, "share_consumer_new: %s\n", errstr);
                exit(2);
        }
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic,
                                          RD_KAFKA_PARTITION_UA);
        err = rd_kafka_share_subscribe(rks, topics);
        rd_kafka_topic_partition_list_destroy(topics);
        CHECK(!err, "subscribe %s (%s)", topic, rd_kafka_err2str(err));
        return rks;
}

/* Poll until at least `want` records arrive or `deadline_ms` passes.
 * Returns the batch (caller destroys) or NULL. */
static rd_kafka_messages_t *poll_batch(rd_kafka_share_t *rks,
                                       size_t want,
                                       int deadline_ms,
                                       rd_kafka_error_t **errorp) {
        int waited = 0;
        *errorp    = NULL;
        while (waited < deadline_ms) {
                rd_kafka_messages_t *batch = NULL;
                rd_kafka_error_t *error    = rd_kafka_share_poll(rks, 1000, &batch);
                waited += 1000;
                if (error) {
                        *errorp = error;
                        return NULL;
                }
                if (batch && rd_kafka_messages_count(batch) >= want)
                        return batch;
                if (batch) /* fewer than wanted: return what we have */
                        return batch;
        }
        return NULL;
}

int main(int argc, char **argv) {
        char topic[64], group[64];
        rd_kafka_share_t *rks;
        rd_kafka_messages_t *batch;
        rd_kafka_error_t *error;
        rd_kafka_topic_partition_list_t *parts = NULL;
        size_t i, cnt;
        long long stamp = (long long)time(NULL);

        if (argc > 1)
                brokers = argv[1];
        snprintf(topic, sizeof(topic), "q-probe-%lld", stamp);
        snprintf(group, sizeof(group), "sg-probe-%lld", stamp);

        printf("librdkafka %s, brokers %s, topic %s, group %s\n",
               rd_kafka_version_str(), brokers, topic, group);

        check_properties(group);

        printf("[4] group config via IncrementalAlterConfigs\n");
        set_group_earliest(group);

        printf("[2] explicit acknowledgement\n");
        produce(topic, 5);
        rks   = new_share_consumer(group, topic);
        batch = poll_batch(rks, 5, 30000, &error);
        cnt   = batch ? rd_kafka_messages_count(batch) : 0;
        CHECK(cnt == 5, "first poll returned %zu records (%s)", cnt,
              error ? rd_kafka_error_string(error) : "no error");
        if (error)
                rd_kafka_error_destroy(error);

        for (i = 0; i < cnt; i++) {
                rd_kafka_message_t *m = rd_kafka_messages_get(batch, i);
                rd_kafka_share_AcknowledgeType_t type =
                    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT;
                rd_kafka_resp_err_t err;
                CHECK(!m->err && rd_kafka_message_delivery_count(m) == 1,
                      "offset %lld delivery_count=%d err=%s",
                      (long long)m->offset, rd_kafka_message_delivery_count(m),
                      rd_kafka_err2str(m->err));
                if (m->offset == 1)
                        type = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
                else if (m->offset == 2)
                        type = RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT;
                else if (m->offset == 4)
                        continue; /* left unacknowledged for check 3 */
                /* By topic/partition/offset - the way the component acks */
                err = rd_kafka_share_acknowledge_offset(
                    rks, rd_kafka_topic_name(m->rkt), m->partition, m->offset,
                    type);
                CHECK(!err, "acknowledge_offset %lld type %d (%s)",
                      (long long)m->offset, (int)type, rd_kafka_err2str(err));
        }

        printf("[3] poll with an unacknowledged record\n");
        {
                rd_kafka_messages_t *b2 = NULL;
                rd_kafka_error_t *e2    = rd_kafka_share_poll(rks, 1000, &b2);
                CHECK(e2 && rd_kafka_error_code(e2) == RD_KAFKA_RESP_ERR__STATE,
                      "poll returned %s: %s",
                      e2 ? rd_kafka_err2name(rd_kafka_error_code(e2)) : "NULL",
                      e2 ? rd_kafka_error_string(e2) : "");
                if (e2)
                        rd_kafka_error_destroy(e2);
                rd_kafka_messages_destroy(b2);
        }
        {
                rd_kafka_resp_err_t err = rd_kafka_share_acknowledge_offset(
                    rks, topic, 0, 4, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE);
                CHECK(!err, "release offset 4 (%s)", rd_kafka_err2str(err));
                /* librdkafka does not reject a second ack before commit: the
                 * last type wins, so offset 4 ends up ACCEPTed. The component
                 * must reject repeated acks itself. */
                err = rd_kafka_share_acknowledge_offset(
                    rks, topic, 0, 4, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                CHECK(!err, "second ack of offset 4 overrides RELEASE (%s)",
                      rd_kafka_err2str(err));
                err = rd_kafka_share_acknowledge_offset(
                    rks, topic, 0, 999, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                CHECK(err == RD_KAFKA_RESP_ERR__STATE,
                      "ack of unknown offset 999 rejected (%s)",
                      rd_kafka_err2str(err));
        }
        rd_kafka_messages_destroy(batch);

        error = rd_kafka_share_commit_sync(rks, 10000, &parts);
        CHECK(!error, "commit_sync (%s)",
              error ? rd_kafka_error_string(error) : "ok");
        if (error)
                rd_kafka_error_destroy(error);
        if (parts) {
                int p;
                for (p = 0; p < parts->cnt; p++)
                        CHECK(!parts->elems[p].err, "commit %s [%d]: %s",
                              parts->elems[p].topic, parts->elems[p].partition,
                              rd_kafka_err2str(parts->elems[p].err));
                rd_kafka_topic_partition_list_destroy(parts);
        }

        printf("[2b] redelivery of released records\n");
        batch = poll_batch(rks, 1, 30000, &error);
        cnt   = batch ? rd_kafka_messages_count(batch) : 0;
        if (error) {
                CHECK(0, "redelivery poll: %s", rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        }
        {
                int seen1 = 0, other = 0;
                for (i = 0; i < cnt; i++) {
                        rd_kafka_message_t *m = rd_kafka_messages_get(batch, i);
                        printf("  INFO redelivered offset %lld delivery_count=%d\n",
                               (long long)m->offset,
                               rd_kafka_message_delivery_count(m));
                        if (m->offset == 1 &&
                            rd_kafka_message_delivery_count(m) == 2)
                                seen1 = 1;
                        else
                                other = 1;
                        rd_kafka_share_acknowledge_offset(
                            rks, rd_kafka_topic_name(m->rkt), m->partition,
                            m->offset, RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT);
                }
                CHECK(seen1 && !other,
                      "only released offset 1 redelivered with "
                      "delivery_count=2 (rejected 2, accepted 4 not)");
        }
        rd_kafka_messages_destroy(batch);

        error = rd_kafka_share_consumer_close(rks);
        CHECK(!error, "close (%s)", error ? rd_kafka_error_string(error) : "ok");
        if (error)
                rd_kafka_error_destroy(error);
        rd_kafka_share_destroy(rks);

        printf("%s: %d failure(s)\n", failures ? "FAILED" : "PASSED", failures);
        return failures ? 1 : 0;
}
