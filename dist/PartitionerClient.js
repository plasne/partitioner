"use strict";
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
var PartitionerClient = /** @class */ (function (_super) {
    __extends(PartitionerClient, _super);
    function PartitionerClient(options) {
        var _this = _super.call(this, options) || this;
        _this.partitions = [];
        _this.isUnsettled = false;
        return _this;
    }
    PartitionerClient.prototype.waitOnSettle = function () {
        var _this = this;
        return new Promise(function (resolve) {
            if (_this.isUnsettled) {
                _this.once('settle', function () {
                    resolve();
                });
            }
            else {
                resolve();
            }
        });
    };
    PartitionerClient.prototype.unsettle = function () {
        this.isUnsettled = true;
    };
    PartitionerClient.prototype.settle = function () {
        this.isUnsettled = false;
        this.emit('settle');
    };
    PartitionerClient.prototype.connect = function () {
        var _this = this;
        // handle "assign"
        this.on('cmd:assign', function (payload, respond) {
            try {
                var partitions = Array.isArray(payload) ? payload : [payload];
                var _loop_1 = function (partition) {
                    var existing = _this.partitions.find(function (p) { return p.id === partition.id; });
                    if (!existing) {
                        _this.partitions.push(partition);
                        _this.emit('assign', partition);
                    }
                };
                for (var _i = 0, partitions_1 = partitions; _i < partitions_1.length; _i++) {
                    var partition = partitions_1[_i];
                    _loop_1(partition);
                }
            }
            catch (error) {
                _this.emit('error', error, 'assign');
            }
            if (respond)
                respond();
        });
        // handle "unassign"
        //  returns an array of partitions with the most up-to-date pointer
        this.on('cmd:unassign', function (payload, respond) { return __awaiter(_this, void 0, void 0, function () {
            var closed, partitions, _loop_2, this_1, _i, partitions_2, partition, error_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        closed = [];
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        partitions = Array.isArray(payload)
                            ? payload
                            : [payload];
                        // wait on fetch to get the final pointer point
                        return [4 /*yield*/, this.waitOnSettle()];
                    case 2:
                        // wait on fetch to get the final pointer point
                        _a.sent();
                        _loop_2 = function (partition) {
                            var index = this_1.partitions.findIndex(function (p) { return p.id === partition.id; });
                            if (index > -1) {
                                this_1.partitions.splice(index, 1);
                                this_1.emit('unassign', partition);
                                closed.push(partition);
                            }
                        };
                        this_1 = this;
                        // remove each
                        for (_i = 0, partitions_2 = partitions; _i < partitions_2.length; _i++) {
                            partition = partitions_2[_i];
                            _loop_2(partition);
                        }
                        return [3 /*break*/, 4];
                    case 3:
                        error_1 = _a.sent();
                        this.emit('error', error_1, 'unassign');
                        return [3 /*break*/, 4];
                    case 4:
                        if (respond)
                            respond(closed);
                        return [2 /*return*/];
                }
            });
        }); });
        // handle "ask" (for which partitions the client would like)
        this.on('cmd:ask', function (_, respond) {
            try {
                _this.sendCommand('assign', _this.partitions);
            }
            catch (error) {
                _this.emit('error', error, 'unassign');
            }
            if (respond)
                respond(closed);
        });
        // connect
        _super.prototype.connect.call(this);
    };
    return PartitionerClient;
}(tcp_comm_1.TcpClient));
exports.PartitionerClient = PartitionerClient;
