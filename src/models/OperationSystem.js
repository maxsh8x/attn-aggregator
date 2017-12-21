const mongoose = require("mongoose");
const AutoIncrement = require("mongoose-sequence")(mongoose);

Data = mongoose.Schema({
  name: String,
  code: Number
});

Data.plugin(AutoIncrement, {
  inc_field: "code",
  id: "operation_system_seq"
});

Data.index({ code: 1 }, { unique: true });
Data.index({ name: 1 }, { unique: true });

module.exports = mongoose.model("OperationSystem", Data);
