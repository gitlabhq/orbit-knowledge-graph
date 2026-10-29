import { Rpc } from '@opencode/plugin/rpc'

export type Activity = {
  /** The tool call ID, which pairs a start with its end. */
  key: string
  state: 'start' | 'end'
  /** The running label, for example "Exploring Orbit". */
  active: string
  /** The finished label, for example "Explored Orbit". */
  done: string
  /** What the call reads, for example "Merge requests → Project". */
  detail: string
  sessionID: string
  failed?: boolean
  /** Nodes the response returned, plus the nodes a count(...) aggregate covered. */
  nodes?: number
  edges?: number
}

export const OrbitUi = Rpc.define<{ activity: Activity }>({
  id: 'orbit.ui',
  methods: {},
  events: {
    activity: {
      schema: {
        type: 'object',
        properties: {
          key: { type: 'string' },
          state: { type: 'string', enum: ['start', 'end'] },
          active: { type: 'string' },
          done: { type: 'string' },
          detail: { type: 'string' },
          sessionID: { type: 'string' },
          failed: { type: 'boolean' },
          nodes: { type: 'integer', minimum: 0 },
          edges: { type: 'integer', minimum: 0 },
        },
        required: ['key', 'state', 'active', 'done', 'detail', 'sessionID'],
        additionalProperties: false,
      },
    },
  },
})
