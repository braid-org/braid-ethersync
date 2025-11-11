
var braid_fetch = require('braid-http').fetch

var url = 'https://braid.org/apps/ethersync'
var cursor_url = 'https://braid.org/.well-known/cursor/apps/ethersync'

var peer = Math.random().toString(36).slice(2)

// Main execution
const server = new EthersyncServer();
server.start();

function EthersyncServer() {
  this.openFiles = new Map(); // uri -> file state
  this.nextId = 1;
  this.sseClients = new Set(); // Store SSE connections

  Object.assign(this, {
    start() {
      const server = require('http').createServer((req, res) => {
        // Enable CORS
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

        if (req.method === 'OPTIONS') {
          res.writeHead(200);
          res.end();
          return;
        }

        if (req.url === '/rpc' && req.method === 'POST') {
          this.handleRPC(req, res);
        } else if (req.url === '/events' && req.method === 'GET') {
          this.handleSSE(req, res);
        } else {
          res.writeHead(404);
          res.end('Not found');
        }
      });

      server.listen(1009, () => {
        console.error('Ethersync server listening on http://localhost:1009');
      });
    },

    handleSSE(req, res) {
      // Set up SSE headers
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no' // Disable Nginx buffering
      });

      // Send initial ping to establish connection
      res.write(':ping\n\n');

      // Add client to set
      this.sseClients.add(res);
      console.error('SSE client connected. Total clients:', this.sseClients.size);

      // Set up ping to keep connection alive
      const pingInterval = setInterval(() => {
        res.write(':ping\n\n');
      }, 30000);

      // Clean up on disconnect
      req.on('close', () => {
        this.sseClients.delete(res);
        clearInterval(pingInterval);
        console.error('SSE client disconnected. Total clients:', this.sseClients.size);
      });
    },

    handleRPC(req, res) {
      let body = '';
      
      req.on('data', (chunk) => {
        body += chunk;
      });
      
      req.on('end', () => {
        try {
          const message = JSON.parse(body);

          console.error('Received RPC:', JSON.stringify(message, null, 2));
          
          // Process the message
          if (message.method === 'open') {
            this.handleOpen(message);
          } else if (message.method === 'close') {
            this.handleClose(message);
          } else if (message.method === 'edit') {
            this.handleEdit(message);
          } else if (message.method === 'cursor') {
            this.handleCursor(message);
          }
          
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'ok' }));
        } catch (e) {
          console.error('Failed to process RPC:', e);
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
    },

    handleOpen(message) {
      var { uri, content } = message.params;

      // safety check for now
      if (!uri.endsWith('/zz')) {
        throw new Error('invalid file for now')
      }
      
      // Initialize file state
      const fileState = {
        editorRevision: 0,
        last_seen_revision: 0,
        braidClient: null
      };

      this.openFiles.set(uri, fileState);

      // Create simpleton client connection to braid.org
      const braidClient = simpleton_client(url, {

        // for braid.org/apps/ethersync,
        // we need content-type text/plain
        content_type: 'text/plain',

        on_delta: (delta) => {
          const currentState = this.openFiles.get(uri);
          if (!currentState) return;
          // Send edit notification via SSE
          const edit = {
            jsonrpc: '2.0',
            method: 'edit',
            params: {
              uri: uri,
              revision: currentState.editorRevision,
              delta
            }
          };
          this.broadcastMessage(edit);
        },
      });

      fileState.braidClient = braidClient;

      // Create cursor client
      braid_fetch(cursor_url, {
        subscribe: true,
        peer,
        headers: {
          'selection-sharing-prototype': 'true',
          'Accept': 'text/plain' // necessary for braid.org
          // 'Cookie': 'client=...'
        }
      }).then(r => {
        r.subscribe(update => {
          console.log(`got update = ${JSON.stringify(update.body_text, null, 4)}`)

          var x = JSON.parse(update.body_text)
          for (var [userid, selection] of Object.entries(x)) {
            this.broadcastMessage({
              jsonrpc: '2.0',
              method: 'cursor',
              params: {
                uri,
                userid,
                name: selection.name || 'Test Name',
                ranges: selection.ranges.map(x => {
                  return {
                    start: offset_to_line_char(x[0], braidClient.get_unconfirmed_text()),
                    end: offset_to_line_char(x[1], braidClient.get_unconfirmed_text()),
                  }
                })
              }
            });                      
          }
        }, (e) => {
          console.log('ERR: ', e)
        })
      })

      // Send success response
      this.broadcastMessage({
        jsonrpc: '2.0',
        id: message.id,
        result: { success: true }
      });

      // clear away initial contents for now..
      var initial_content = require('fs').readFileSync(uri.slice('file://'.length), 'utf8')
      console.log(`removing initial content of: ${initial_content}`)
      this.broadcastMessage({
        jsonrpc: '2.0',
        method: 'edit',
        params: {
          uri: uri,
          revision: fileState.editorRevision,
          delta: [{
            range: {
              start: offset_to_line_char(0, initial_content),
              end: offset_to_line_char([...initial_content].length, initial_content)
            },
            replacement: ''
          }]
        }
      });
      fileState.last_seen_revision = 1

      console.error(`Opened file: ${uri}, connected to braid`);
    },

    handleClose(message) {
      const { uri } = message.params;
      
      const fileState = this.openFiles.get(uri);
      if (fileState) {
        // Stop braid client
        if (fileState.braidClient && fileState.braidClient.stop) {
          fileState.braidClient.stop();
        }
        this.openFiles.delete(uri);
      }

      // Send success response
      this.broadcastMessage({
        jsonrpc: '2.0',
        id: message.id,
        result: { success: true }
      });
      
      console.error(`Closed file: ${uri}`);
    },

    handleEdit(message) {
      const { uri, revision, delta } = message.params;
      
      const fileState = this.openFiles.get(uri);
      if (!fileState) throw new Error(`Edit received for unknown file: ${uri}`)
      console.error(`Editor edit received for ${uri}, revision ${revision}`);

      if (revision < fileState.last_seen_revision) throw new Error('user edited too soon')
      fileState.braidClient.changed(revision - fileState.last_seen_revision, delta)
      fileState.last_seen_revision = revision
      fileState.editorRevision++;

      // Send success response
      this.broadcastMessage({
        jsonrpc: '2.0',
        id: message.id,
        result: { success: true }
      });
    },

    handleCursor(message) {
      const { uri, ranges } = message.params;

      const fileState = this.openFiles.get(uri);
      if (!fileState) throw new Error(`Cursor received for unknown file: ${uri}`)
      console.error(`Cursor edit received for ${uri}`);

      braid_fetch(cursor_url, {
        method: 'PUT',
        peer,
        headers: {
          'selection-sharing-prototype': 'true',
          'Content-Type': 'text/plain', // necessary for braid.org
          // 'Cookie': 'client=...'
        },
        body: JSON.stringify({
          [peer]: {
            ranges: ranges.map(x => {
              return [
                line_char_to_offset(x.start, fileState.braidClient.get_unconfirmed_text()),
                line_char_to_offset(x.end, fileState.braidClient.get_unconfirmed_text())
              ]
            }),
            time: Date.now()
          }
        })
      })

      // Send success response
      this.broadcastMessage({
        jsonrpc: '2.0',
        id: message.id,
        result: { success: true }
      });
    },

    broadcastMessage(message) {
      const data = `data: ${JSON.stringify(message)}\n\n`;
      
      // Send to all connected SSE clients
      for (const client of this.sseClients) {
        try {
          client.write(data);
        } catch (e) {
          console.error('Failed to send to SSE client:', e);
          // Client will be removed when 'close' event fires
        }
      }
      
      console.error('Broadcasted to', this.sseClients.size, 'clients, the message: ' + JSON.stringify(message, null, 4));
    }
  })
}

function simpleton_client(url, {
    on_delta,
    content_type,
    on_error,
}) {
    var peer = Math.random().toString(36).slice(2)
    var current_version = []
    var current_text = ''
    var current_patches = []
    var char_counter = -1
    var outstanding_changes = 0
    var max_outstanding_changes = 10
    var ac = new AbortController()

    var unconfirmed_updates = []
    var unconfirmed_text = current_text

    braid_fetch(url, {
        headers: { "Merge-Type": "simpleton",
            ...(content_type ? {Accept: content_type} : {}) },
        subscribe: true,
        retry: () => true,
        parents: () => current_version.length ? current_version : null,
        peer,
        signal: ac.signal
    }).then(res => {
        res.subscribe(async update => {
            if (!current_patches.length &&
                arrays_equal(update.parents.sort(),
                    unconfirmed_updates.at(-1)?.version ||
                    current_version)) {

                var version = update.version.sort()

                var patches = update.patches ||
                    [{range: [0, 0], content: update.body_text}]
                if (update.patches) for (let p of patches) {
                    p.range = p.range.match(/\d+/g).map((x) => 1 * x)
                    p.content = p.content_text
                }

                // hack: deal with patches one at a time..
                patches = absolute_to_relative_patches(patches)
                for (var p of patches) {
                    var delta = [{
                        range: {
                            start: offset_to_line_char(p.range[0], unconfirmed_text),
                            end: offset_to_line_char(p.range[1], unconfirmed_text),
                        },
                        replacement: p.content
                    }]

                    unconfirmed_updates.push({ version, patches: [p] })
                    unconfirmed_text = apply_patches(unconfirmed_text, [p])

                    on_delta(delta)
                }

                // var delta = patches.map(p => ({
                //     range: {
                //         start: offset_to_line_char(p.range[0], unconfirmed_text),
                //         end: offset_to_line_char(p.range[1], unconfirmed_text),
                //     },
                //     replacement: p.content
                // }))

                // unconfirmed_updates.push({ version, patches })
                // unconfirmed_text = apply_patches(unconfirmed_text, patches)

                // on_delta(delta)
            }
        }, on_error)
    }).catch(on_error)
    
    return {
      stop: async () => {
        ac.abort()
      },
      get_unconfirmed_text: () => unconfirmed_text,
      get_current_text: () => current_text,
      changed: async (num_confirmed_updates, delta) => {
        // get up to speed from unconfirmed updates..
        for (var u of unconfirmed_updates.slice(0, num_confirmed_updates)) {
          current_version = u.version
          current_text = apply_patches(current_text, u.patches)
        }
        unconfirmed_updates = []

        // apply this latest delta..
        var patches = delta.map(d => ({
            range: [
                line_char_to_offset(d.range.start, current_text),
                line_char_to_offset(d.range.end, current_text)
            ],
            content: d.replacement
        })).sort((a, b) => a.range[0] - b.range[0])

        current_text = apply_patches(current_text, patches)
        unconfirmed_text = current_text

        current_patches = relative_to_absolute_patches(
          absolute_to_relative_patches(current_patches).concat(
            absolute_to_relative_patches(patches)))

        if (outstanding_changes >= max_outstanding_changes) return
        while (true) {
            if (!current_patches.length) return
            patches = current_patches
            current_patches = []

            char_counter += patches.reduce((a, b) =>
              a + (b.range[1] - b.range[0]) + count_code_points(b.content), 0)

            var version = [peer + "-" + char_counter]
            var parents = current_version
            current_version = version
            
            for (var p of patches) {
              p.unit = 'text'
              p.range = `[${p.range.join(':')}]`
            }

            outstanding_changes++
            try {
                var r = await braid_fetch(url, {
                    headers: {
                        "Merge-Type": "simpleton",
                        ...(content_type ? {"Content-Type": content_type} : {}),
                        // "Cookie": 'client=...'
                    },
                    method: "PUT",
                    retry: (res) => res.status !== 550,
                    version, parents, patches,
                    peer
                })
                if (!r.ok) throw new Error(`bad http status: ${r.status}${(r.status === 401 || r.status === 403) ? ` (access denied)` : ''}`)
            } catch (e) {
                on_error(e)
                throw e
            }
            outstanding_changes--
        }
      }
    }
}

// Utility functions

function line_char_to_offset({line, character}, context) {
    var currentLine = 0
    var currentLineStart = 0
    var codePointIndex = 0
    var i = 0

    while (true) {
        if (currentLine === line &&
            codePointIndex - currentLineStart === character)
            return codePointIndex

        if (i >= context.length) throw new Error(`Line ${line}, character ${character} is beyond the text`)

        var char = context[i]

        codePointIndex++
        if (char === "\r" || char === "\n") {
            if (char === "\r" && i + 1 < context.length && context[i + 1] === "\n") {
                i++
                codePointIndex++
            }
            currentLine++
            currentLineStart = codePointIndex
        } else {
            var codeUnit = context.charCodeAt(i)
            if (codeUnit >= 0xd800 && codeUnit <= 0xdbff && i + 1 < context.length) {
                var nextCodeUnit = context.charCodeAt(i + 1)
                if (nextCodeUnit >= 0xdc00 && nextCodeUnit <= 0xdfff) {
                    // Surrogate pair - single code point
                    i += 2
                    continue
                }
            }
        }
        i++
    }
}

function offset_to_line_char(offset, context) {
    offset_to_line_char.was_between_crlf = false

    let currentLine = 0
    let currentLineStart = 0
    let codePointIndex = 0
    let i = 0

    while (true) {
        if (codePointIndex === offset || i >= context.length)
            return {
                line: currentLine,
                character: codePointIndex - currentLineStart,
            }

        const char = context[i]

        codePointIndex++
        if (char === "\r" || char === "\n") {
            if (char === "\r" && i + 1 < context.length && context[i + 1] === "\n") {
                // \r\n sequence - check if offset falls between them
                if (codePointIndex === offset) {
                    offset_to_line_char.was_between_crlf = true
                    return {
                        line: currentLine,
                        character: codePointIndex - currentLineStart - 1,
                    }
                }
                i++
                codePointIndex++
            }
            currentLine++
            currentLineStart = codePointIndex
        } else {
            const codeUnit = context.charCodeAt(i)
            if (codeUnit >= 0xd800 && codeUnit <= 0xdbff && i + 1 < context.length) {
                const nextCodeUnit = context.charCodeAt(i + 1)
                if (nextCodeUnit >= 0xdc00 && nextCodeUnit <= 0xdfff) {
                    i += 2
                    continue
                }
            }
        }
        i++
    }
}

function arrays_equal(a, b) {
  return a.length === b.length && a.every((v, i) => v === b[i])
}

function apply_patches(s, patches) {
    // Sort patches by start position in descending order
    // This ensures we apply from right to left, avoiding position shifts
    for (let p of [...patches].sort((a, b) => b.range[0] - a.range[0]))
        s = s.slice(0, codePoints_to_index(s, p.range[0])) +
            p.content +
            s.slice(codePoints_to_index(s, p.range[1]))
    return s
}

function relative_to_absolute_patches(patches) {
    let avl = create_avl_tree((node) => {
        let parent = node.parent
        if (parent.left == node) {
            parent.left_size -= node.left_size + node.size
        } else {
            node.left_size += parent.left_size + parent.size
        }
    })
    avl.root.size = Infinity
    avl.root.left_size = 0

    function resize(node, new_size) {
        if (node.size == new_size) return
        let delta = new_size - node.size
        node.size = new_size
        while (node.parent) {
            if (node.parent.left == node) node.parent.left_size += delta
            node = node.parent
        }
    }

    for (let p of patches) {
        let [start, end] = p.range
        let del = end - start

        let node = avl.root
        while (true) {
            if (start < node.left_size || (node.left && node.content == null && start == node.left_size)) {
                node = node.left
            } else if (start > node.left_size + node.size || (node.content == null && start == node.left_size + node.size)) {
                start -= node.left_size + node.size
                node = node.right
            } else {
                start -= node.left_size
                break
            }
        }

        let remaining = start + del - node.size
        if (remaining < 0) {
            if (node.content == null) {
                if (start > 0) {
                    let x = { size: 0, left_size: 0 }
                    avl.add(node, "left", x)
                    resize(x, start)
                }
                let x = { size: 0, left_size: 0, content: p.content, del }
                avl.add(node, "left", x)
                resize(x, count_code_points(x.content))
                resize(node, node.size - (start + del))
            } else {
                node.content = node.content.slice(0, codePoints_to_index(node.content, start)) + p.content + node.content.slice(codePoints_to_index(node.content, start + del))
                resize(node, count_code_points(node.content))
            }
        } else {
            let next
            let middle_del = 0
            while (remaining >= (next = avl.next(node)).size) {
                remaining -= next.size
                middle_del += next.del ?? next.size
                resize(next, 0)
                avl.del(next)
            }

            if (node.content == null) {
                if (next.content == null) {
                    if (start == 0) {
                        node.content = p.content
                        node.del = node.size + middle_del + remaining
                        resize(node, count_code_points(node.content))
                    } else {
                        let x = {
                            size: 0,
                            left_size: 0,
                            content: p.content,
                            del: node.size - start + middle_del + remaining,
                        }
                        resize(node, start)
                        avl.add(node, "right", x)
                        resize(x, count_code_points(x.content))
                    }
                    resize(next, next.size - remaining)
                } else {
                    next.del += node.size - start + middle_del
                    next.content = p.content + next.content.slice(codePoints_to_index(next.content, remaining))
                    resize(node, start)
                    if (node.size == 0) avl.del(node)
                    resize(next, count_code_points(next.content))
                }
            } else {
                if (next.content == null) {
                    node.del += middle_del + remaining
                    node.content = node.content.slice(0, codePoints_to_index(node.content, start)) + p.content
                    resize(node, count_code_points(node.content))
                    resize(next, next.size - remaining)
                } else {
                    node.del += middle_del + next.del
                    node.content = node.content.slice(0, codePoints_to_index(node.content, start)) + p.content + next.content.slice(codePoints_to_index(next.content, remaining))
                    resize(node, count_code_points(node.content))
                    resize(next, 0)
                    avl.del(next)
                }
            }
        }
    }

    let new_patches = []
    let offset = 0
    let node = avl.root
    while (node.left) node = node.left
    while (node) {
        if (node.content == null) {
            offset += node.size
        } else {
            new_patches.push({
                unit: patches[0].unit,
                range: [offset, offset + node.del],
                content: node.content,
            })
            offset += node.del
        }

        node = avl.next(node)
    }
    return new_patches
}

function create_avl_tree(on_rotate) {
    let self = { root: { height: 1 } }

    self.calc_height = (node) => {
        node.height = 1 + Math.max(node.left?.height ?? 0, node.right?.height ?? 0)
    }

    self.rechild = (child, new_child) => {
        if (child.parent) {
            if (child.parent.left == child) {
                child.parent.left = new_child
            } else {
                child.parent.right = new_child
            }
        } else {
            self.root = new_child
        }
        if (new_child) new_child.parent = child.parent
    }

    self.rotate = (node) => {
        on_rotate(node)

        let parent = node.parent
        let left = parent.right == node ? "left" : "right"
        let right = parent.right == node ? "right" : "left"

        parent[right] = node[left]
        if (parent[right]) parent[right].parent = parent
        self.calc_height(parent)

        self.rechild(parent, node)
        parent.parent = node

        node[left] = parent
    }

    self.fix_avl = (node) => {
        self.calc_height(node)
        let diff = (node.right?.height ?? 0) - (node.left?.height ?? 0)
        if (Math.abs(diff) >= 2) {
            if (diff > 0) {
                if ((node.right.left?.height ?? 0) > (node.right.right?.height ?? 0)) self.rotate(node.right.left)
                self.rotate((node = node.right))
            } else {
                if ((node.left.right?.height ?? 0) > (node.left.left?.height ?? 0)) self.rotate(node.left.right)
                self.rotate((node = node.left))
            }
            self.fix_avl(node)
        } else if (node.parent) self.fix_avl(node.parent)
    }

    self.add = (node, side, add_me) => {
        let other_side = side == "left" ? "right" : "left"
        add_me.height = 1

        if (node[side]) {
            node = node[side]
            while (node[other_side]) node = node[other_side]
            node[other_side] = add_me
        } else {
            node[side] = add_me
        }
        add_me.parent = node
        self.fix_avl(node)
    }

    self.del = (node) => {
        if (node.left && node.right) {
            let cursor = node.right
            while (cursor.left) cursor = cursor.left
            cursor.left = node.left

            // breaks abstraction
            cursor.left_size = node.left_size
            let y = cursor
            while (y.parent != node) {
                y = y.parent
                y.left_size -= cursor.size
            }

            node.left.parent = cursor
            if (cursor == node.right) {
                self.rechild(node, cursor)
                self.fix_avl(cursor)
            } else {
                let x = cursor.parent
                self.rechild(cursor, cursor.right)
                cursor.right = node.right
                node.right.parent = cursor
                self.rechild(node, cursor)
                self.fix_avl(x)
            }
        } else {
            self.rechild(node, node.left || node.right || null)
            if (node.parent) self.fix_avl(node.parent)
        }
    }

    self.next = (node) => {
        if (node.right) {
            node = node.right
            while (node.left) node = node.left
            return node
        } else {
            while (node.parent && node.parent.right == node) node = node.parent
            return node.parent
        }
    }

    return self
}

function count_code_points(str) {
    let code_points = 0;
    for (let i = 0; i < str.length; i++) {
        if (str.charCodeAt(i) >= 0xD800 && str.charCodeAt(i) <= 0xDBFF) i++;
        code_points++;
    }
    return code_points;
}

function codePoints_to_index(str, codePoints) {
    let i = 0;
    let c = 0;
    while (c < codePoints && i < str.length) {
        const charCode = str.charCodeAt(i);
        i += (charCode >= 0xd800 && charCode <= 0xdbff) ? 2 : 1;
        c++;
    }
    return i;
}

function absolute_to_relative_patches(patches) {
    // Sort patches by start position (guaranteed non-overlapping)
    const sortedPatches = [...patches].sort((a, b) => {
        return a.range[0] - b.range[0];
    });
    
    const relativePatches = [];
    let cumulativeOffset = 0;
    
    for (const patch of sortedPatches) {
        const [start, end] = patch.range;
        const deletedLength = end - start;
        const insertedLength = count_code_points(patch.content);
        
        // Adjust positions based on cumulative offset from previous patches
        const relativeStart = start - cumulativeOffset;
        const relativeEnd = end - cumulativeOffset;
        
        relativePatches.push({
            range: [relativeStart, relativeEnd],
            content: patch.content
        });
        
        // Update cumulative offset for next patches
        // Offset = total inserted - total deleted
        cumulativeOffset += deletedLength - insertedLength;
    }
    
    return relativePatches;
}
