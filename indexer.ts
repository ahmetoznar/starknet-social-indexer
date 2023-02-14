import { NodeClient, credentials, proto } from "@apibara/protocol";
import { Block, Event, TransactionReceipt } from "@apibara/starknet";
const BN = require("bn.js");
const starknet = require("starknet");
import { PrismaClient } from "@prisma/client";
const db = new PrismaClient();

function hexToBuffer(h: string): Buffer {
  return Buffer.from(h.replace("0x", "").padStart(64, "0"), "hex");
}

function bufferToHex(b: Buffer | Uint8Array): string {
  return Buffer.from(b).toString("hex");
}

function uint256FromBytes(low: Buffer, high: Buffer) {
  const lowB = new BN(low);
  const highB = new BN(high);
  return highB.shln(128).add(lowB);
}

function h2d(s: any) {
  function add(x: any, y: any) {
    var c = 0,
      r = [];
    var x = x.split("").map(Number);
    var y = y.split("").map(Number);
    while (x.length || y.length) {
      var s = (x.pop() || 0) + (y.pop() || 0) + c;
      r.unshift(s < 10 ? s : s - 10);
      c = s < 10 ? 0 : 1;
    }
    if (c) r.unshift(c);
    return r.join("");
  }

  var dec = "0";
  s.split("").forEach(function (chr: any) {
    var n = parseInt(chr, 16);
    for (var t = 8; t; t >>= 1) {
      dec = add(dec, dec);
      if (n & t) dec = add(dec, "1");
    }
  });
  return dec;
}

function bufferToNumber(value: any) {
  const hex_value = bufferToHex(Buffer.from(value));
  return h2d(hex_value);
}

interface EventObject {
  event_name: string;
  event_name_buffer: Buffer;
}
class AppIndexer {
  address: Buffer;
  eventName_one: EventObject;
  eventName_two: EventObject;
  eventName_three: EventObject;

  constructor(
    address: string,
    event_one: string,
    event_two: string,
    event_three: string
  ) {
    this.address = hexToBuffer(address);
    this.eventName_one = {
      event_name: event_one,
      event_name_buffer: hexToBuffer(
        starknet.hash.getSelectorFromName(event_one)
      ),
    };
    this.eventName_two = {
      event_name: event_two,
      event_name_buffer: hexToBuffer(
        starknet.hash.getSelectorFromName(event_two)
      ),
    };
    this.eventName_three = {
      event_name: event_three,
      event_name_buffer: hexToBuffer(
        starknet.hash.getSelectorFromName(event_three)
      ),
    };
  }

  // protobuf encodes possibly-large numbers as strings
  private currentSequence?: string;

  async handleData(data: any) {
    console.log(`[data] sequence=${data.sequence}`);
    // track sequence number for reconnecting later
    const block = Block.decode(data.data.value);
    for (let index = 0; index < block.transactionReceipts.length; index++) {
      var tx_receipt: TransactionReceipt = block.transactionReceipts[index];
      const tx_hash = tx_receipt.transactionHash;
      for (let event of tx_receipt.events) {
        try {
          if (!this.address.equals(event.fromAddress)) {
            continue;
          }

          if (this.eventName_one.event_name_buffer.equals(event.keys[0])) {
            //new_domain_registered
            console.log(bufferToNumber(event.data[5]), "avatar hash ___");
            let event_obj = {
              token_id: uint256FromBytes(
                Buffer.from(event.data[0]),
                Buffer.from(event.data[1])
              ).toString(),
              domain: bufferToNumber(event.data[2]),
              resolver_address: starknet.number.cleanHex(
                bufferToHex(Buffer.from(event.data[3]))
              ),
              expire_date: bufferToNumber(event.data[4]),
              avatar_hash: bufferToNumber(event.data[6]),
            };
            try {
              await db.domains.upsert({
                where: {
                  domain: event_obj.domain,
                },
                create: event_obj,
                update: {
                  token_id: event_obj.token_id,
                  resolver_address: event_obj.resolver_address,
                  expire_date: event_obj.expire_date,
                  avatar_hash: event_obj.avatar_hash,
                },
              });
              const is_inited = await db.profile_info.upsert({
                where: {
                  profile_domain_uuid: event_obj.domain,
                },
                create: {
                  profile_domain_uuid: event_obj.domain,
                  is_inited: true,
                },
                update: {
                  is_inited: true,
                },
              });
            } catch (error) {
              console.log(error, "error message");
            }
            console.log(
              event_obj,
              "== [EVENT_CAPTURED] == new_domain_registered"
            );
            console.log("Found block for:", data.sequence);
            console.log("Txn at:", bufferToHex(Buffer.from(tx_hash)));
          }

          if (this.eventName_two.event_name_buffer.equals(event.keys[0])) {
            //domain_updated
            const hex_selector =
              starknet.hash.getSelectorFromName("domain_updated");

            console.log(hex_selector, "selector - domain_updated");
            console.log(bufferToHex(event.keys[0]), "event name");

            let event_obj = {
              token_id: uint256FromBytes(
                Buffer.from(event.data[0]),
                Buffer.from(event.data[1])
              ).toString(),
              domain: bufferToNumber(event.data[2]),
              old_owner: starknet.number.cleanHex(
                bufferToHex(Buffer.from(event.data[3]))
              ),
              new_owner: starknet.number.cleanHex(
                bufferToHex(Buffer.from(event.data[4]))
              ),
              emitted_at: bufferToNumber(event.data[5]),
            };
            try {
              await db.domains.update({
                where: {
                  token_id: event_obj.token_id,
                },
                data: {
                  token_id: event_obj.token_id,
                  domain: event_obj.domain,
                  resolver_address: event_obj.new_owner,
                  expire_date: undefined,
                },
              });
            } catch (error) {
              console.log(error, "error message");
            }
            console.log(event_obj, "== [EVENT_CAPTURED] == domain_updated");
            console.log("Found block for:", data.sequence);
            console.log("Txn at:", bufferToHex(Buffer.from(tx_hash)));
          }

          if (this.eventName_three.event_name_buffer.equals(event.keys[0])) {
            //domain_renewed
            let event_obj = {
              token_id: uint256FromBytes(
                Buffer.from(event.data[0]),
                Buffer.from(event.data[1])
              ).toString(),
              domain: bufferToNumber(Buffer.from(event.data[2])),
              resolver: starknet.number.cleanHex(
                bufferToHex(Buffer.from(event.data[3]))
              ),
              expire_date: bufferToNumber(event.data[4]),
              emitted_at: bufferToNumber(event.data[5]),
              avatar_hash: bufferToNumber(event.data[6]),
            };

            try {
              await db.domains.update({
                where: {
                  token_id: event_obj.token_id,
                },
                data: {
                  token_id: event_obj.token_id,
                  domain: event_obj.domain,
                  resolver_address: event_obj.resolver,
                  expire_date: event_obj.expire_date,
                  avatar_hash: event_obj.avatar_hash,
                },
              });
            } catch (error) {
              console.log(error, "error message");
            }

            console.log(event_obj, "== [EVENT_CAPTURED] == domain_renewed");
            console.log("Found block for:", data.sequence);
            console.log("Txn at:", bufferToHex(Buffer.from(tx_hash)));
          }
        } catch (error) {
          console.log(error, "<=== ERROR LOGGED");
        }
      }
    }
  }

  handleInvalidate(invalidate: proto.Invalidate__Output) {
    console.log(`[invalidate] sequence=${invalidate.sequence}`);
    this.currentSequence = invalidate.sequence;
  }

  onRetry(retryCount: number) {
    // retry connecting up to 3 times, with a delay of 5 seconds in between
    // retries.
    // Start from the sequence number _following_ the last received message.
    const retry = retryCount < 300;
    const startingSequence = this.currentSequence
      ? +this.currentSequence + 1
      : undefined;

    console.log(
      `[retry] retry=${
        retry ? "yes" : "no"
      }, startingSequence=${startingSequence}`
    );

    return { retry, delay: 20, startingSequence };
  }
}
async function main() {
  const indexer = new AppIndexer(
    "0x787cdee55592dcee421147c599db0abd73f418dae9dc7804ae026b8f914960a",
    "new_domain_registered",
    "domain_updated",
    "domain_renewed"
  );
  const node = new NodeClient(
    "goerli.starknet.stream.apibara.com:443",
    credentials.createSsl()
  );

  const messages = node.streamMessages(
    { startingSequence: 735_118 },
    {
      reconnect: true,
      onRetry: indexer.onRetry,
    }
  );

  return new Promise((resolve, reject) => {
    messages.on("end", resolve);
    messages.on("error", reject);
    messages.on("data", (msg: any) => {
      if (msg.data) {
        indexer.handleData(msg.data);
      } else if (msg.invalidate) {
        indexer.handleInvalidate(msg.invalidate);
      }
    });
  });
}
main();
