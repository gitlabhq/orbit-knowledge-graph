<script context="module" lang="ts">
export const prerender = true;
</script>
<script lang="ts">
import { writable } from "svelte/store";
const count = writable(0);
</script>
