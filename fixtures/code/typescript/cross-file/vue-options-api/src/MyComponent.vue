<script>
import { formatDate, validate } from './utils';

export default {
  name: 'MyComponent',
  props: {
    item: { type: Object, required: true },
  },
  computed: {
    formattedDate() {
      return formatDate(this.item.date);
    },
  },
  methods: {
    handleClick() {
      if (validate(this.item.name)) {
        this.submitForm();
      }
    },
    submitForm() {
      this.$emit('submit', this.item);
    },
  },
};
</script>
