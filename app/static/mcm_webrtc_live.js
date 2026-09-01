/**
 * Minimal MCM WebRTC consumer (same JSON signalling as Blue Robotics Cockpit).
 * Signalling WebSocket: ws(s)://<vehicle-host>:6021/  (REST remains on :6020).
 */
(function (global) {
  'use strict';

  var RTC_CFG = {
    bundlePolicy: 'max-bundle',
    iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
  };

  function signallingWsUrl() {
    var host = window.location.hostname;
    var secure = window.location.protocol === 'https:';
    var scheme = secure ? 'wss' : 'ws';
    return scheme + '://' + host + ':6021/';
  }

  function _normMatchUrl(u) {
    if (u == null) return '';
    var s = String(u).trim();
    if (!s) return '';
    try {
      var x = new URL(s);
      x.hash = '';
      return x.toString().toLowerCase();
    } catch (e) {
      return s.toLowerCase();
    }
  }

  function _normName(n) {
    return String(n == null ? '' : n)
      .trim()
      .replace(/\s+/g, ' ')
      .toLowerCase();
  }

  function parseMessage(data) {
    try {
      return JSON.parse(data);
    } catch (e) {
      return null;
    }
  }

  function McmWebRtcLive(videoEl, hooks) {
    this.videoEl = videoEl;
    this.onStatus = (hooks && hooks.onStatus) || function () {};
    this.onError = (hooks && hooks.onError) || function () {};
    this._ws = null;
    this._consumerId = null;
    this._sessionId = null;
    this._producerId = null;
    this._pc = null;
    this._negHandler = null;
    this._connecting = false;
  }

  McmWebRtcLive.prototype.close = function () {
    this._endPeer();
    if (this._ws) {
      try {
        this._ws.onopen = null;
        this._ws.onerror = null;
        this._ws.onclose = null;
        if (this._negHandler) this._ws.removeEventListener('message', this._negHandler);
      } catch (e) {}
      try {
        this._ws.close();
      } catch (e2) {}
    }
    this._ws = null;
    this._consumerId = null;
    this._sessionId = null;
    this._producerId = null;
    this._connecting = false;
  };

  McmWebRtcLive.prototype._send = function (obj) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) throw new Error('WebSocket not open');
    this._ws.send(JSON.stringify(obj));
  };

  McmWebRtcLive.prototype._waitAnswer = function (matchFn, timeoutMs) {
    var ws = this._ws;
    var self = this;
    return new Promise(function (resolve, reject) {
      var done = false;
      var t = setTimeout(function () {
        if (done) return;
        done = true;
        ws.removeEventListener('message', onMsg);
        reject(new Error('Timed out waiting for signalling answer'));
      }, timeoutMs || 12000);
      function onMsg(ev) {
        if (done) return;
        var msg = parseMessage(ev.data);
        if (!msg || msg.type !== 'answer') return;
        try {
          if (matchFn(msg)) {
            done = true;
            clearTimeout(t);
            ws.removeEventListener('message', onMsg);
            resolve(msg);
          }
        } catch (e) {
          /* keep listening */
        }
      }
      ws.addEventListener('message', onMsg);
    });
  };

  McmWebRtcLive.prototype.connect = function () {
    var self = this;
    if (self._connecting || (self._ws && self._ws.readyState === WebSocket.OPEN)) {
      return Promise.resolve();
    }
    self._connecting = true;
    return new Promise(function (resolve, reject) {
      var url = signallingWsUrl();
      self.onStatus('Signalling: ' + url);
      var ws = new WebSocket(url);
      self._ws = ws;
      ws.onerror = function () {
        self._connecting = false;
        self.onError('WebSocket failed (' + url + '). Is MCM signalling on port 6021?');
        reject(new Error('ws error'));
      };
      ws.onclose = function () {
        self._connecting = false;
      };
      ws.onopen = function () {
        self._connecting = false;
        self
          ._waitAnswer(function (m) {
            var a = m.content;
            return a && a.type === 'peerId' && a.content && a.content.id;
          }, 10000)
          .then(function (m) {
            self._consumerId = m.content.content.id;
            self.onStatus('Signalling ready (consumer ' + self._consumerId.slice(0, 8) + '…)');
            resolve();
          })
          .catch(reject);
        try {
          self._send({ type: 'question', content: { type: 'peerId' } });
        } catch (e) {
          reject(e);
        }
      };
    });
  };

  McmWebRtcLive.prototype._fetchAvailableStreams = function () {
    var self = this;
    var p = self._waitAnswer(function (m) {
      var a = m.content;
      return a && a.type === 'availableStreams' && Array.isArray(a.content);
    }, 12000);
    self._send({ type: 'question', content: { type: 'availableStreams' } });
    return p.then(function (m) {
      return m.content.content;
    });
  };

  McmWebRtcLive.prototype._findProducer = function (streams, streamUuid, streamName, rtspUrl) {
    var uuid = streamUuid == null ? '' : String(streamUuid);
    var name = streamName == null ? '' : String(streamName).trim();
    var nameLo = name.toLowerCase();
    var nameNorm = _normName(name);
    var urlNorm = _normMatchUrl(rtspUrl);

    // REST /streams "id" is often not the same as WebRTC producer "id"; match by name first.
    var s = null;
    if (name) {
      s = streams.find(function (x) {
        return x && x.name && String(x.name).trim() === name;
      });
      if (!s && nameLo) {
        s = streams.find(function (x) {
          return x && x.name && String(x.name).trim().toLowerCase() === nameLo;
        });
      }
      if (!s && nameNorm) {
        s = streams.find(function (x) {
          return x && x.name && _normName(x.name) === nameNorm;
        });
      }
    }
    if (!s && uuid) {
      s = streams.find(function (x) {
        return x && String(x.id) === uuid;
      });
    }
    if (!s && uuid && name) {
      s = streams.find(function (x) {
        return x && x.name && String(x.name).indexOf(uuid) !== -1;
      });
    }
    if (!s && urlNorm) {
      s = streams.find(function (x) {
        return x && x.source && _normMatchUrl(x.source) === urlNorm;
      });
    }
    return s || null;
  };

  McmWebRtcLive.prototype._endPeer = function () {
    if (this._negHandler && this._ws) {
      try {
        this._ws.removeEventListener('message', this._negHandler);
      } catch (e) {}
    }
    this._negHandler = null;
    if (this._pc) {
      try {
        this._pc.getSenders().forEach(function (s) {
          try {
            if (s.track) s.track.stop();
          } catch (e) {}
        });
        this._pc.close();
      } catch (e2) {}
    }
    this._pc = null;
    this._sessionId = null;
    this._producerId = null;
    if (this.videoEl) {
      try {
        this.videoEl.srcObject = null;
      } catch (e3) {}
    }
  };

  McmWebRtcLive.prototype.playPreferredH264 = function (nameHint) {
    var self = this;
    if (!self._ws || self._ws.readyState !== WebSocket.OPEN || !self._consumerId) {
      return Promise.reject(new Error('Signalling not ready'));
    }
    self._endPeer();
    self.onStatus('Requesting stream list…');
    return self._fetchAvailableStreams().then(function (streams) {
      if (!streams || !streams.length) {
        throw new Error('No MCM WebRTC producers. Create a stream in BlueOS Video Streams, then retry.');
      }
      var hint = nameHint || null;
      var h264 = streams.filter(function (s) {
        return s && String(s.encode || '').toUpperCase() === 'H264';
      });
      var pool = h264.length ? h264 : streams;
      var prod = null;
      if (hint) {
        prod = pool.find(function (s) {
          return s && s.name && hint.test(String(s.name));
        });
      }
      prod = prod || pool[0];
      if (!prod || !prod.id) {
        throw new Error(
          'No usable MCM stream. Available: ' +
            streams
              .map(function (x) {
                return (x && x.name ? x.name : '?') + '/' + (x && x.encode ? x.encode : '?');
              })
              .join('; ')
        );
      }
      self._producerId = prod.id;
      self.onStatus('Starting WebRTC session for "' + (prod.name || prod.id) + '"…');
      var p = self._waitAnswer(function (m) {
        var a = m.content;
        if (!a || a.type !== 'startSession' || !a.content) return false;
        var c = a.content;
        return c.session_id && c.consumer_id === self._consumerId && c.producer_id === self._producerId;
      }, 20000);
      self._send({
        type: 'question',
        content: {
          type: 'startSession',
          content: { consumer_id: self._consumerId, producer_id: self._producerId },
        },
      });
      return p.then(function (m) {
        self._sessionId = m.content.content.session_id;
        self._startPeerConnection();
      });
    });
  };

  McmWebRtcLive.prototype.playStream = function (streamUuid, streamName, rtspUrl) {
    var self = this;
    if (!self._ws || self._ws.readyState !== WebSocket.OPEN || !self._consumerId) {
      return Promise.reject(new Error('Signalling not ready'));
    }
    self._endPeer();
    self.onStatus('Requesting stream list…');
    return self
      ._fetchAvailableStreams()
      .then(function (streams) {
        var prod = self._findProducer(streams, streamUuid, streamName, rtspUrl);
        if (!prod) {
          var hint = '';
          try {
            hint =
              ' Available: ' +
              streams
                .map(function (x) {
                  return (x && x.name ? x.name : '?') + '(' + (x && x.id ? x.id : '?') + ')';
                })
                .join('; ');
          } catch (e2) {
            hint = '';
          }
          throw new Error(
            'Stream not found in MCM WebRTC list (REST id ' +
              streamUuid +
              (streamName ? ', name "' + streamName + '"' : '') +
              ').' +
              hint
          );
        }
        self._producerId = prod.id;
        self.onStatus('Starting WebRTC session for "' + prod.name + '"…');
        var p = self._waitAnswer(function (m) {
          var a = m.content;
          if (!a || a.type !== 'startSession' || !a.content) return false;
          var c = a.content;
          return c.session_id && c.consumer_id === self._consumerId && c.producer_id === self._producerId;
        }, 15000);
        self._send({
          type: 'question',
          content: {
            type: 'startSession',
            content: { consumer_id: self._consumerId, producer_id: self._producerId },
          },
        });
        return p.then(function (m) {
          self._sessionId = m.content.content.session_id;
          self._startPeerConnection();
        });
      });
  };

  McmWebRtcLive.prototype._startPeerConnection = function () {
    var self = this;
    var pc = new RTCPeerConnection(RTC_CFG);
    self._pc = pc;
    pc.addTransceiver('video', { direction: 'recvonly' });

    pc.ontrack = function (ev) {
      var ms = ev.streams && ev.streams[0];
      if (ms && self.videoEl) {
        self.videoEl.srcObject = ms;
        var p = self.videoEl.play();
        if (p && p.catch) p.catch(function () {});
      }
    };

    pc.onicecandidate = function (ev) {
      if (!ev.candidate || !self._ws || self._ws.readyState !== WebSocket.OPEN) return;
      try {
        self._send({
          type: 'negotiation',
          content: {
            type: 'iceNegotiation',
            content: {
              session_id: self._sessionId,
              consumer_id: self._consumerId,
              producer_id: self._producerId,
              ice: ev.candidate.toJSON(),
            },
          },
        });
      } catch (e) {}
    };

    pc.onconnectionstatechange = function () {
      self.onStatus('Peer: ' + pc.connectionState);
      if (pc.connectionState === 'failed') {
        self.onError('WebRTC connection failed');
      }
    };

    self._negHandler = function (ev) {
      var msg = parseMessage(ev.data);
      if (!msg || msg.type !== 'negotiation') return;
      var neg = msg.content;
      if (!neg || !neg.content) return;
      var c = neg.content;
      if (c.session_id !== self._sessionId || c.consumer_id !== self._consumerId || c.producer_id !== self._producerId) {
        return;
      }
      if (neg.type === 'iceNegotiation' && c.ice) {
        pc.addIceCandidate(c.ice).catch(function () {});
      } else if (neg.type === 'mediaNegotiation' && c.sdp) {
        var desc = new RTCSessionDescription(c.sdp);
        pc.setRemoteDescription(desc)
          .then(function () {
            return pc.createAnswer();
          })
          .then(function (answer) {
            return pc.setLocalDescription(answer);
          })
          .then(function () {
            if (!self._ws || self._ws.readyState !== WebSocket.OPEN) return;
            self._send({
              type: 'negotiation',
              content: {
                type: 'mediaNegotiation',
                content: {
                  session_id: self._sessionId,
                  consumer_id: self._consumerId,
                  producer_id: self._producerId,
                  sdp: pc.localDescription,
                },
              },
            });
          })
          .catch(function (err) {
            self.onError('SDP negotiation failed: ' + (err && err.message ? err.message : err));
          });
      }
    };
    self._ws.addEventListener('message', self._negHandler);
  };

  global.McmWebRtcLive = {
    create: function (videoEl, hooks) {
      return new McmWebRtcLive(videoEl, hooks);
    },
    signallingWsUrl: signallingWsUrl,
  };
})(typeof window !== 'undefined' ? window : this);
