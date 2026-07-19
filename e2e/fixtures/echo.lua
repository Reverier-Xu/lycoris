-- Echo extension fixture (test asset, not a production extension): returns
-- the invoked method and the payload it received.
function invoke(method, payload)
  return { method = method, payload = payload }
end
