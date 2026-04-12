function normalize(value) {
  return value.trim().toLowerCase();
}

function validate(value) {
  return value.length > 0;
}

module.exports = { normalize, validate };
