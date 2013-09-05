// Code adapted from https://github.com/Operational-Transformation/ot.js/
// Licensed under the terms of the MIT license.

if (typeof ot === "undefined") { var ot = {}; }

(function () {
    function ClientDocument(revision) {
        this.revision = revision;
        this.state = ClientDocument.synchronized;
    }

    ClientDocument.prototype = {
        constructor: ClientDocument,

        applyClient: function (op) {
            this.state = this.state.applyClient(this, op);
        },

        applyServer: function (op) {
            this.revision++;
            this.state = this.state.applyServer(this, op);
        },

        serverAck: function () {
            this.revision++;
            this.state = this.state.serverAck(this);
        }
    };

    ClientDocument.synchronized = {
        applyClient: function (client, op) {
            client.sendOperation(client.revision, op);
            return new ClientDocument.AwaitingConfirm(op);
        },

        applyServer: function (client, op) {
            client.applyOperation(op);
            return this;
        },

        serverAck: function (client) {
            throw "no pending operation to ack";
        }
    };

    function AwaitingConfirm(outstanding) {
        this.outstanding = outstanding;
    }

    ClientDocument.AwaitingConfirm = AwaitingConfirm;

    AwaitingConfirm.prototype = {
        constructor: AwaitingConfirm,

        applyClient: function (client, op) {
            return new ClientDocument.AwaitingWithBuffer(this.outstanding, op);
        },

        applyServer: function (client, op) {
            var pair = op.constructor.transform(this.outstanding, op);
            client.applyOperation(pair[1]);
            return new ClientDocument.AwaitingConfirm(pair[0]);
        },

        serverAck: function (client) {
            return ClientDocument.synchronized;
        }
    };

    function AwaitingWithBuffer(outstanding, buffer) {
        this.outstanding = outstanding;
        this.buffer = buffer;
    }

    ClientDocument.AwaitingWithBuffer = AwaitingWithBuffer;

    AwaitingWithBuffer.prototype = {
        constructor: AwaitingWithBuffer,

        applyClient: function (client, op) {
            return new ClientDocument.AwaitingWithBuffer(this.outstanding,
                                                         this.buffer.compose(op));
        },

        applyServer: function (client, op) {
            var pair1 = op.constructor.transform(this.outstanding, op);
            var pair2 = op.constructor.transform(this.buffer, pair1[1]);
            client.applyOperation(pair2[1]);
            return new ClientDocument.AwaitingWithBuffer(pair1[0], pair2[0]);
        },

        serverAck: function (client) {
            client.sendOperation(client.revision, this.buffer);
            return new ClientDocument.AwaitingConfirm(this.buffer);
        }
    };

    ot.ClientDocument = ClientDocument;
})();
