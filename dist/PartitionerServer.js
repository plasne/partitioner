"use strict";
// TODO: needs to support committing checkpoints to some kind of state
// TODO: need to document stuff better
// TODO: support clients sending unassign if they want to gracefully shutdown
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    }
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
var tcp_comm_1 = require("tcp-comm");
/* tslint:enable */
// define server logic
var PartitionerServer = /** @class */ (function (_super) {
    __extends(PartitionerServer, _super);
    function PartitionerServer(options) {
        var _this = _super.call(this, options) || this;
        _this.partitions = [];
        _this.isLearning = false;
        // options or defaults
        _this.options = options || {};
        if (options) {
            var local = _this.options;
            local.rebalance = tcp_comm_1.TcpServer.toInt(local.rebalance);
            local.imbalance = tcp_comm_1.TcpServer.toInt(local.imbalance);
        }
        return _this;
    }
    Object.defineProperty(PartitionerServer.prototype, "rebalanceEvery", {
        get: function () {
            var local = this.options;
            return local.rebalance || 10000;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(PartitionerServer.prototype, "allowedImbalance", {
        get: function () {
            var local = this.options;
            return local.imbalance || 0;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(PartitionerServer.prototype, "learnFor", {
        get: function () {
            var local = this.options;
            return local.learnFor || 60000;
        },
        enumerable: true,
        configurable: true
    });
    PartitionerServer.prototype.listen = function () {
        var _this = this;
        // once listening has started, start timed processes
        this.on('listen', function () {
            // start rebalancing
            _this.scheduleRebalance();
            // start learning
            if (_this.learnFor > 0) {
                _this.emit('learning');
                _this.isLearning = true;
                setTimeout(function () {
                    _this.isLearning = false;
                    _this.emit('managing');
                }, _this.learnFor);
            }
            else {
                _this.emit('managing');
            }
        });
        // when clients connect, give them all their current partitions
        this.on('connect', function (client) {
            if (client.socket) {
                // on connect, send an list of partitions that are currently assigned
                var partitions = _this.partitions.reduce(function (array, partition) {
                    if (partition.client === client) {
                        array.push({
                            id: partition.id,
                            pointer: partition.pointer
                        });
                    }
                    return array;
                }, []);
                if (partitions.length > 0) {
                    _this.tell(client, 'assign', partitions);
                }
                // if learning, send a request to the client to tell of any partitions it knows of
                if (_this.isLearning) {
                    _this.tell(client, 'ask');
                }
            }
        });
        // propogate more explict naming
        this.on('add', function (client) {
            _this.emit('add-client', client);
        });
        this.on('remove', function (client) {
            _this.emit('remove-client', client);
        });
        // allow clients to add partitions
        this.on('cmd:add-partition', function (_, payload, respond) {
            try {
                var partitions = Array.isArray(payload)
                    ? payload
                    : [payload];
                var _loop_1 = function (partition) {
                    var existing = _this.partitions.find(function (p) { return p.id === partition.id; });
                    if (!existing) {
                        _this.partitions.push(partition);
                        _this.emit('add-partition', partition);
                    }
                };
                for (var _i = 0, partitions_1 = partitions; _i < partitions_1.length; _i++) {
                    var partition = partitions_1[_i];
                    _loop_1(partition);
                }
            }
            catch (error) {
                _this.emit('error', error, 'add');
            }
            if (respond)
                respond();
        });
        // allow clients to remove partitions
        this.on('cmd:remove-partition', function (_, payload, respond) {
            try {
                var partitions = Array.isArray(payload)
                    ? payload
                    : [payload];
                var _loop_2 = function (partition) {
                    var index = _this.partitions.findIndex(function (p) { return p.id === partition.id; });
                    if (index > -1) {
                        _this.partitions.splice(index, 1);
                        _this.emit('remove-partition', partition);
                    }
                };
                for (var _i = 0, partitions_2 = partitions; _i < partitions_2.length; _i++) {
                    var partition = partitions_2[_i];
                    _loop_2(partition);
                }
            }
            catch (error) {
                _this.emit('error', error, 'remove');
            }
            if (respond)
                respond();
        });
        // accept partitions from clients during learning mode
        this.on('cmd:assign', function (client, payload, respond) {
            try {
                var partitions = Array.isArray(payload)
                    ? payload
                    : [payload];
                var _loop_3 = function (partition) {
                    var existing = _this.partitions.find(function (p) { return p.id === partition.id; });
                    if (!existing) {
                        _this.partitions.push(partition);
                        _this.emit('add-partition', partition);
                        partition.client = client;
                        _this.emit('assign', partition);
                    }
                    else if (!existing.client) {
                        existing.client = client;
                        _this.emit('assign', existing);
                    }
                };
                for (var _i = 0, partitions_3 = partitions; _i < partitions_3.length; _i++) {
                    var partition = partitions_3[_i];
                    _loop_3(partition);
                }
            }
            catch (error) {
                _this.emit('error', error, 'assign');
            }
            if (respond)
                respond();
        });
        // start listening
        _super.prototype.listen.call(this);
    };
    PartitionerServer.prototype.counts = function () {
        var counts = [];
        var _loop_4 = function (client) {
            var count = this_1.partitions.filter(function (p) {
                if (p.yieldTo) {
                    return p.yieldTo.id === client.id;
                }
                else {
                    return p.client && p.client.id === client.id;
                }
            }).length;
            counts.push({
                client: client,
                count: count
            });
        };
        var this_1 = this;
        for (var _i = 0, _a = this.clients; _i < _a.length; _i++) {
            var client = _a[_i];
            _loop_4(client);
        }
        return counts;
    };
    PartitionerServer.prototype.rebalance = function () {
        var _this = this;
        try {
            // TODO: right now if there are more clients than partitions, nothing rebalances
            //   this is due to (< min) where min is 0
            // do not rebalance while learning
            if (this.isLearning) {
                this.scheduleRebalance();
                return;
            }
            // clear any clients that are not connected
            for (var _i = 0, _a = this.clients; _i < _a.length; _i++) {
                var client = _a[_i];
                if (!client.socket) {
                    for (var _b = 0, _c = this.partitions; _b < _c.length; _b++) {
                        var partition = _c[_b];
                        if (partition.client &&
                            partition.client.id === client.id) {
                            this.emit('unassign', partition);
                            partition.client = undefined;
                        }
                    }
                    this.remove(client);
                }
            }
            // if there are no clients; there is nothing to rebalance
            if (this.clients.length > 0) {
                // count all partitions using a client
                var counts_2 = this.counts();
                // determine the minimum number that should be allocated per client
                var min = Math.floor(this.partitions.length / this.clients.length);
                // function to assign unassigned
                var assign = function (partition) {
                    counts_2.sort(function (a, b) {
                        return a.count - b.count;
                    });
                    partition.client = counts_2[0].client;
                    _this.emit('assign', partition);
                    counts_2[0].count++;
                    if (partition.client.socket) {
                        _this.tell(partition.client, 'assign', {
                            id: partition.id,
                            pointer: partition.id
                        });
                    }
                };
                // assign unassigned
                for (var _d = 0, _e = this.partitions; _d < _e.length; _d++) {
                    var partition = _e[_d];
                    if (!partition.client)
                        assign(partition);
                }
                // function to reassign the last appearance of a superior
                var reassign = function (to) {
                    counts_2.sort(function (a, b) {
                        return b.count - a.count;
                    });
                    for (var i = _this.partitions.length - 1; i >= 0; i--) {
                        var partition = _this.partitions[i];
                        if (!partition.yieldTo &&
                            partition.client === counts_2[0].client) {
                            partition.yieldTo = to;
                            _this.emit('yield', partition);
                            counts_2[0].count--;
                            return;
                        }
                    }
                };
                // reassign as needed to ensure minimums
                //  NOTE: this uses yield, does not immediately assign
                for (var _f = 0, counts_1 = counts_2; _f < counts_1.length; _f++) {
                    var entry = counts_1[_f];
                    while (min - entry.count > this.allowedImbalance) {
                        reassign(entry.client);
                        entry.count++;
                    }
                }
                // yield for anything still outstanding (yield should be IDEMPOTENT)
                for (var _g = 0, _h = this.partitions; _g < _h.length; _g++) {
                    var partition = _h[_g];
                    if (partition.yieldTo) {
                        this.yield(partition);
                    }
                }
            }
            // emit a successful rebalance
            this.emit('rebalance');
        }
        catch (error) {
            this.emit('error', error, 'rebalance');
        }
        this.scheduleRebalance();
    };
    PartitionerServer.prototype.addPartition = function (partition) {
        var existing = this.partitions.find(function (p) { return p.id === partition.id; });
        if (!existing) {
            this.partitions.push(partition);
            this.emit('add-partition', partition);
        }
    };
    PartitionerServer.prototype.removePartition = function (partition) {
        var index = this.partitions.indexOf(partition);
        if (index > -1) {
            this.partitions.splice(index, 1);
            this.emit('remote-partition', partition);
        }
    };
    PartitionerServer.prototype.addClient = function (client) {
        _super.prototype.add.call(this, client);
    };
    PartitionerServer.prototype.removeClient = function (client) {
        _super.prototype.remove.call(this, client);
    };
    PartitionerServer.prototype.yield = function (partition) {
        return __awaiter(this, void 0, void 0, function () {
            var unassign, assign, updated;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        unassign = function () { return __awaiter(_this, void 0, void 0, function () {
                            var closed_1, error_1;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0:
                                        _a.trys.push([0, 3, , 4]);
                                        if (!(partition.client && partition.client.socket)) return [3 /*break*/, 2];
                                        return [4 /*yield*/, this.ask(partition.client, 'unassign', {
                                                id: partition.id
                                            })];
                                    case 1:
                                        closed_1 = _a.sent();
                                        // unassign
                                        this.emit('unassign', partition);
                                        partition.client = undefined;
                                        // the most up-to-date pointer is returned
                                        return [2 /*return*/, closed_1.find(function (p) { return p.id === partition.id; })];
                                    case 2: return [3 /*break*/, 4];
                                    case 3:
                                        error_1 = _a.sent();
                                        this.emit('error', error_1, 'reassign:unassign');
                                        return [3 /*break*/, 4];
                                    case 4: return [2 /*return*/, undefined];
                                }
                            });
                        }); };
                        assign = function () { return __awaiter(_this, void 0, void 0, function () {
                            var error_2;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0:
                                        _a.trys.push([0, 3, , 4]);
                                        if (!(partition.yieldTo && partition.yieldTo.socket)) return [3 /*break*/, 2];
                                        return [4 /*yield*/, this.ask(partition.yieldTo, 'assign', {
                                                id: partition.id,
                                                pointer: partition.pointer
                                            })];
                                    case 1:
                                        _a.sent();
                                        // assign
                                        partition.client = partition.yieldTo;
                                        partition.yieldTo = undefined;
                                        this.emit('assign', partition);
                                        _a.label = 2;
                                    case 2: return [3 /*break*/, 4];
                                    case 3:
                                        error_2 = _a.sent();
                                        this.emit('error', error_2, 'reassign:assign');
                                        return [3 /*break*/, 4];
                                    case 4: return [2 /*return*/];
                                }
                            });
                        }); };
                        return [4 /*yield*/, unassign()];
                    case 1:
                        updated = _a.sent();
                        if (updated && updated.pointer)
                            partition.pointer = updated.pointer;
                        return [4 /*yield*/, assign()];
                    case 2:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    PartitionerServer.prototype.scheduleRebalance = function () {
        var _this = this;
        setTimeout(function () {
            _this.rebalance();
        }, this.rebalanceEvery);
    };
    return PartitionerServer;
}(tcp_comm_1.TcpServer));
exports.PartitionerServer = PartitionerServer;
