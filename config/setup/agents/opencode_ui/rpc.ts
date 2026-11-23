import { Rpc } from '@opencode/plugin/rpc'

export type Activity = {
  key: string
  state: 'start' | 'end'
  active: string
  done: string
  detail: string
  others?: string
  sessionID: string
  failed?: boolean
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
          others: { type: 'string' },
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
