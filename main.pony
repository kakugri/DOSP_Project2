use "collections"
use "random"
use "time"

actor Main
  var numNodes: U64
  let topology: Stringable
  let algorithm: Stringable

  new create(env: Env) =>
    if env.args.size() < 4 then
      env.out.print("Usage: ./project2 <numNodes> <topology> <algorithm>")
      numNodes = 40
      topology = "topology"
      algorithm = "algorithm"
      return
    end
  
  numNodes = try env.args(1)?.u64()? else 40 end
  topology = try env.args(2)? else "Line" end
  algorithm = try env.args(3)? else "Gossip" end
  var startTime: U64 = Time.millis()

  if algorithm.string() == "Gossip" then
    match topology
    | "Line" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      let triplets = recover val 
        var dummy: Array[U64] = [0; 0; 0]
        dummy
        end
      let nodePool = recover val
        var temp: Array[Node] = Array[Node].create()
        for i in Range[U64](0, numNodes) do
          temp.push(Node((i + 1), numNodes, triplets, startTime))
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?.spreadRumorGossipLine(nodePool, env) end
    | "Full Network" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      let triplets = recover val 
        var dummy: Array[U64] = [0; 0; 0]
        dummy
        end
      let nodePool = recover val
        var temp: Array[Node] = Array[Node].create()
        for i in Range[U64](0, numNodes) do
          temp.push(Node((i + 1), numNodes, triplets, startTime))
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?.spreadRumorGossipFull(nodePool, env) end
    | "3d Grid" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      var x_cord: U64 = U64(numNodes).f64().cbrt().ceil().u64()
      var y_cord: U64 = (U64(numNodes).f64()/U64(x_cord).f64()).sqrt().ceil().u64()
      var z_cord: U64 = (U64(numNodes).f64()/U64(x_cord * y_cord).f64()).ceil().u64()
      var x_final: U64 = 0
      var y_final: U64 = 0
      var z_final: U64 = 0
      y_cord = x_cord
      z_cord = x_cord
      numNodes = (x_cord * x_cord) * x_cord
      let nodePool = recover val
        var temp: Array[Array[Array[Node]]] = Array[Array[Array[Node]]].create()
        var counter: U64 = 0
        for i in Range[USize](0, x_cord.usize()) do
          if counter >= numNodes then
              break
          end
          var layer: Array[Array[Node]] = Array[Array[Node]].create()
          for j in Range[USize](0, y_cord.usize()) do
            if counter >= numNodes then
              break
            end
            var row: Array[Node] = Array[Node].create()
            for k in Range[USize](0, z_cord.usize()) do
              counter = counter + 1
              let triplets = recover val 
                var dummy: Array[U64] = [i.u64(); j.u64(); k.u64()]
                dummy
                end
              row.push(Node((counter), numNodes, triplets, startTime))
              if counter >= numNodes then
                x_final = i.u64()
                y_final = j.u64()
                z_final = k.u64()
                break
              end
            end
            layer.push(row)
          end
          temp.push(layer)
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(x_cord).usize())?(U64(0).usize())?(U64(0).usize())?.spreadRumorGossip3DGrid(nodePool, env, x_cord, y_cord, z_cord) end
    | "Imp3d Grid" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      var x_cord: U64 = U64(numNodes).f64().cbrt().ceil().u64()
      var y_cord: U64 = (U64(numNodes).f64()/U64(x_cord).f64()).sqrt().ceil().u64()
      var z_cord: U64 = (U64(numNodes).f64()/U64(x_cord * y_cord).f64()).ceil().u64()
      var x_final: U64 = 0
      var y_final: U64 = 0
      var z_final: U64 = 0
      y_cord = x_cord
      z_cord = x_cord
      numNodes = (x_cord * x_cord) * x_cord
      let nodePool = recover val
        var temp: Array[Array[Array[Node]]] = Array[Array[Array[Node]]].create()
        var counter: U64 = 0
        for i in Range[USize](0, x_cord.usize()) do
          if counter >= numNodes then
              break
          end
          var layer: Array[Array[Node]] = Array[Array[Node]].create()
          for j in Range[USize](0, y_cord.usize()) do
            if counter >= numNodes then
              break
            end
            var row: Array[Node] = Array[Node].create()
            for k in Range[USize](0, z_cord.usize()) do
              counter = counter + 1
              let triplets = recover val 
                var dummy: Array[U64] = [i.u64(); j.u64(); k.u64()]
                dummy
                end
              row.push(Node((counter), numNodes, triplets, startTime))
              if counter >= numNodes then
                x_final = i.u64()
                y_final = j.u64()
                z_final = k.u64()
                break
              end
            end
            layer.push(row)
          end
          temp.push(layer)
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?(U64(0).usize())?(U64(0).usize())?.spreadRumorGossipImp3DGrid(nodePool, env, x_cord, y_cord, z_cord) end
    end
  elseif algorithm.string() == "PushSum" then
    match topology
    | "Line" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      let triplets = recover val 
        var dummy: Array[U64] = [0; 0; 0]
        dummy
        end
      let nodePool = recover val
        var temp: Array[Node] = Array[Node].create()
        for i in Range[U64](0, numNodes) do
          temp.push(Node((i + 1), numNodes, triplets, startTime))
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?.spreadRumorPushSumLine(nodePool, env, U64(0).f64(), U64(0).f64()) end
    | "Full Network" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      let triplets = recover val 
        var dummy: Array[U64] = [0; 0; 0]
        dummy
        end
      let nodePool = recover val
        var temp: Array[Node] = Array[Node].create()
        for i in Range[U64](0, numNodes) do
          temp.push(Node((i + 1), numNodes, triplets, startTime))
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?.spreadRumorPushSumFull(nodePool, env, U64(0).f64(), U64(0).f64()) end
    | "3d Grid" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      var x_cord: U64 = U64(numNodes).f64().cbrt().ceil().u64()
      var y_cord: U64 = (U64(numNodes).f64()/U64(x_cord).f64()).sqrt().ceil().u64()
      var z_cord: U64 = (U64(numNodes).f64()/U64(x_cord * y_cord).f64()).ceil().u64()
      var x_final: U64 = 0
      var y_final: U64 = 0
      var z_final: U64 = 0
      y_cord = x_cord
      z_cord = x_cord
      numNodes = (x_cord * x_cord) * x_cord
      let nodePool = recover val
        var temp: Array[Array[Array[Node]]] = Array[Array[Array[Node]]].create()
        var counter: U64 = 0
        for i in Range[USize](0, x_cord.usize()) do
          if counter >= numNodes then
              break
          end
          var layer: Array[Array[Node]] = Array[Array[Node]].create()
          for j in Range[USize](0, y_cord.usize()) do
            if counter >= numNodes then
              break
            end
            var row: Array[Node] = Array[Node].create()
            for k in Range[USize](0, z_cord.usize()) do
              counter = counter + 1
              let triplets = recover val 
                var dummy: Array[U64] = [i.u64(); j.u64(); k.u64()]
                dummy
              end
              row.push(Node((counter), numNodes, triplets, startTime))
              if counter >= numNodes then
                x_final = i.u64()
                y_final = j.u64()
                z_final = k.u64()
                break
              end
            end
            layer.push(row)
          end
          temp.push(layer)
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?(U64(0).usize())?(U64(0).usize())?.spreadRumorPushSum3DGrid(nodePool, env, x_cord, y_cord, z_cord, U64(0).f64(), U64(0).f64()) end
    | "Imp3d Grid" => 
      env.out.print("Starting " + topology.string() + " " + algorithm.string())
      var x_cord: U64 = U64(numNodes).f64().cbrt().ceil().u64()
      var y_cord: U64 = (U64(numNodes).f64()/U64(x_cord).f64()).sqrt().ceil().u64()
      var z_cord: U64 = (U64(numNodes).f64()/U64(x_cord * y_cord).f64()).ceil().u64()
      var x_final: U64 = 0
      var y_final: U64 = 0
      var z_final: U64 = 0
      y_cord = x_cord
      z_cord = x_cord
      numNodes = (x_cord * x_cord) * x_cord
      let nodePool = recover val
        var temp: Array[Array[Array[Node]]] = Array[Array[Array[Node]]].create()
        var counter: U64 = 0
        for i in Range[USize](0, x_cord.usize()) do
          if counter >= numNodes then
              break
          end
          var layer: Array[Array[Node]] = Array[Array[Node]].create()
          for j in Range[USize](0, y_cord.usize()) do
            if counter >= numNodes then
              break
            end
            var row: Array[Node] = Array[Node].create()
            for k in Range[USize](0, z_cord.usize()) do
              counter = counter + 1
              let triplets = recover val 
                var dummy: Array[U64] = [i.u64(); j.u64(); k.u64()]
                dummy
                end
              row.push(Node((counter), numNodes, triplets, startTime))
              if counter >= numNodes then
                x_final = i.u64()
                y_final = j.u64()
                z_final = k.u64()
                break
              end
            end
            layer.push(row)
          end
          temp.push(layer)
        end
        env.out.print("Finished initialising all nodes")
        temp
      end
      try nodePool(U64(0).usize())?(U64(0).usize())?(U64(0).usize())?.spreadRumorPushSum3DGrid(nodePool, env, x_cord, y_cord, z_cord, U64(0).f64(), U64(0).f64()) end
    end
  end

actor Node
  var name: U64
  var s: F64
  var w: F64 = 1
  var rumorCount: U64 = 0
  var numNodes: U64
  var notAvailable: Bool = false
  var changeCount: U64 = 0
  var triplets: Array[U64] val
  var startTime: U64
  var rand: Rand

  new create(name': U64, numNodes': U64, triplet': Array[U64] val, startTime': U64) =>
    name = name'
    numNodes = numNodes'
    s = U64(name').f64()
    triplets = triplet'
    startTime = startTime'
    rand = Rand.from_u64(Time.cycles())

  be spreadRumorGossipLine(nodePool': Array[Node] val, env: Env) =>
    if notAvailable then
      return
    end
    rumorCount = rumorCount + 1
    if rumorCount >= 10 then
      env.out.print("rumorCount for Node " + name.string() + " is " + rumorCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    if (name > 1) and (name < (numNodes)) then
        var dice1 = rand.int[U64](2)
        if dice1 == 1 then
          try nodePool'(U64(name - 1).usize() - 1)?.spreadRumorGossipLine(nodePool', env) end
        end
        if dice1 == 0 then
          try nodePool'(U64(name - 1).usize() + 1)?.spreadRumorGossipLine(nodePool', env) end
        end
    end
    if name == 1 then
        try nodePool'(U64(name - 1).usize() + 1)?.spreadRumorGossipLine(nodePool', env) end
    end
    if name == (numNodes) then
        try nodePool'(U64(name - 1).usize() - 1)?.spreadRumorGossipLine(nodePool', env) end
    end

  be spreadRumorGossipFull(nodePool': Array[Node] val, env: Env) =>
    if notAvailable then
      return
    end
    rumorCount = rumorCount + 1
    if rumorCount >= 10 then
      env.out.print("rumorCount for Node " + name.string() + " is " + rumorCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    var dice1 = rand.int[U64](numNodes)
    try nodePool'(U64(dice1).usize())?.spreadRumorGossipFull(nodePool', env) end

  be spreadRumorGossip3DGrid(nodePool': Array[Array[Array[Node]]] val, env: Env, x': U64, y': U64, z': U64) =>
    if notAvailable then
      return
    end
    rumorCount = rumorCount + 1
    if rumorCount >= 10 then
      env.out.print("rumorCount for Node " + name.string() + " is " + rumorCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    var dice1 = rand.int[U64](2)
    try
      var x = triplets(0)?
      var y = triplets(1)?
      var z = triplets(2)?
      var outbounds: Bool = true
      while outbounds do
        if dice1 == 1 then
          var dice2 = rand.int[U64](3)
          if dice2 == 0 then
            x = x + 1
          elseif dice2 == 1 then
            y = y + 1
          elseif dice2 == 2 then
            z = z + 1
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        elseif dice1 == 0 then
          var dice2 = rand.int[U64](3)
          if dice2 == 0 then
            x = x - 1
          elseif dice2 == 1 then
            y = y - 1
          elseif dice2 == 2 then
            z = z - 1
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        end
      end
      try nodePool'(x.usize())?(y.usize())?(z.usize())?.spreadRumorGossip3DGrid(nodePool', env, x', y', z') end
    end

  be spreadRumorGossipImp3DGrid(nodePool': Array[Array[Array[Node]]] val, env: Env, x': U64, y': U64, z': U64) =>
    if notAvailable then
      return
    end
    rumorCount = rumorCount + 1
    if rumorCount >= 10 then
      env.out.print("rumorCount for Node " + name.string() + " is " + rumorCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    var dice1 = rand.int[U64](2)
    try
      var x = triplets(0)?
      var y = triplets(1)?
      var z = triplets(2)?
      var outbounds: Bool = true
      while outbounds do
        if dice1 == 1 then
          var dice2 = rand.int[U64](4)
          if dice2 == 0 then
            x = x + 1
          elseif dice2 == 1 then
            y = y + 1
          elseif dice2 == 2 then
            z = z + 1
          elseif dice2 == 3 then
            x = rand.int[U64](x')
            y = rand.int[U64](y')
            z = rand.int[U64](z')
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        elseif dice1 == 0 then
          var dice2 = rand.int[U64](4)
          if dice2 == 0 then
            x = x - 1
          elseif dice2 == 1 then
            y = y - 1
          elseif dice2 == 2 then
            z = z - 1
          elseif dice2 == 3 then
            x = rand.int[U64](x')
            y = rand.int[U64](y')
            z = rand.int[U64](z')
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        end
      end
      try nodePool'(x.usize())?(y.usize())?(z.usize())?.spreadRumorGossipImp3DGrid(nodePool', env, x', y', z') end
    end
  
  be spreadRumorPushSumLine(nodePool': Array[Node] val, env: Env, s': F64, w': F64) =>
    if notAvailable then
      return
    end
    var newS = s + s'
    var newW = w + w'
    var newRatio = (newS / newW)
    var ratio = s / w
    var difference = (newRatio - ratio).abs()
    if difference <= (1 / (U64(10).f64().pow(10))) then
      changeCount = changeCount + 1
    else
      changeCount = 0
    end
    if changeCount >= 3 then
      env.out.print("chnageCount for Node " + name.string() + " is " + changeCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    s = newS
    w = newW
    if (name > 1) and (name < (numNodes)) then
        var dice1 = rand.int[U64](2)
        if dice1 == 1 then
          try nodePool'(U64(name - 1).usize() - 1)?.spreadRumorPushSumLine(nodePool', env, (newW / 2), (newS / 2)) end
        end
        if dice1 == 0 then
          try nodePool'(U64(name - 1).usize() + 1)?.spreadRumorPushSumLine(nodePool', env, (newW / 2), (newS / 2)) end
        end
    end
    if name == 1 then
      try nodePool'(U64(name - 1).usize() + 1)?.spreadRumorPushSumLine(nodePool', env, (newW / 2), (newS / 2)) end
    end
    if name == (numNodes) then
      try nodePool'(U64(name - 1).usize() - 1)?.spreadRumorPushSumLine(nodePool', env, (newW / 2), (newS / 2)) end
    end

  be spreadRumorPushSumFull(nodePool': Array[Node] val, env: Env, s': F64, w': F64) =>
    if notAvailable then
      return
    end
    var newS = s + s'
    var newW = w + w'
    var newRatio = (newS / newW)
    var ratio = s / w
    var difference = (newRatio - ratio).abs()
    if difference <= (1 / (U64(10).f64().pow(10))) then
      changeCount = changeCount + 1
    else
      changeCount = 0
    end
    if changeCount >= 3 then
      env.out.print("chnageCount for Node " + name.string() + " is " + changeCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    s = newS
    w = newW
    var dice1 = rand.int[U64](numNodes)
    while dice1 == (name - 1) do
      rand = Rand.from_u64(Time.cycles())
      dice1 = rand.int[U64](numNodes)
    end
    try nodePool'(dice1.usize() - 1)?.spreadRumorPushSumLine(nodePool', env, (newW / 2), (newS / 2)) end

  be spreadRumorPushSum3DGrid(nodePool': Array[Array[Array[Node]]] val, env: Env, x': U64, y': U64, z': U64, s': F64, w': F64) =>
    if notAvailable then
      return
    end
    var newS = s + s'
    var newW = w + w'
    var newRatio = (newS / newW)
    var ratio = s / w
    var difference = (newRatio - ratio).abs()
    if difference <= (1 / (U64(10).f64().pow(10))) then
      changeCount = changeCount + 1
    else
      changeCount = 0
    end
    if changeCount >= 3 then
      env.out.print("chnageCount for Node " + name.string() + " is " + changeCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    s = newS
    w = newW
    var dice1 = rand.int[U64](2)
    try
      var x = triplets(0)?
      var y = triplets(1)?
      var z = triplets(2)?
      var outbounds: Bool = true
      while outbounds do
        if dice1 == 1 then
          var dice2 = rand.int[U64](3)
          if dice2 == 0 then
            x = x + 1
          elseif dice2 == 1 then
            y = y + 1
          elseif dice2 == 2 then
            z = z + 1
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        elseif dice1 == 0 then
          var dice2 = rand.int[U64](3)
          if dice2 == 0 then
            x = x - 1
          elseif dice2 == 1 then
            y = y - 1
          elseif dice2 == 2 then
            z = z - 1
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        end
      end
      try nodePool'(x.usize())?(y.usize())?(z.usize())?.spreadRumorPushSum3DGrid(nodePool', env, x', y', z', (newW / 2), (newS / 2)) end
    end

  be spreadRumorPushSumImp3DGrid(nodePool': Array[Array[Array[Node]]] val, env: Env, x': U64, y': U64, z': U64, s': F64, w': F64) =>
    if notAvailable then
      return
    end
    var newS = s + s'
    var newW = w + w'
    var newRatio = (newS / newW)
    var ratio = s / w
    var difference = (newRatio - ratio).abs()
    if difference <= (1 / (U64(10).f64().pow(10))) then
      changeCount = changeCount + 1
    else
      changeCount = 0
    end
    if changeCount >= 3 then
      env.out.print("chnageCount for Node " + name.string() + " is " + changeCount.string())
      env.out.print("Terminated at Node " + " " + name.string())
      env.out.print("Program terminated afer: " + (Time.millis() - startTime).string() + " milliseconds")
      notAvailable = true
      return
    end
    s = newS
    w = newW
    var dice1 = rand.int[U64](2)
    try
      var x = triplets(0)?
      var y = triplets(1)?
      var z = triplets(2)?
      var outbounds: Bool = true
      while outbounds do
        if dice1 == 1 then
          var dice2 = rand.int[U64](4)
          if dice2 == 0 then
            x = x + 1
          elseif dice2 == 1 then
            y = y + 1
          elseif dice2 == 2 then
            z = z + 1
          elseif dice2 == 3 then
            x = rand.int[U64](x')
            y = rand.int[U64](y')
            z = rand.int[U64](z')
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        elseif dice1 == 0 then
          var dice2 = rand.int[U64](4)
          if dice2 == 0 then
            x = x - 1
          elseif dice2 == 1 then
            y = y - 1
          elseif dice2 == 2 then
            z = z - 1
          elseif dice2 == 3 then
            x = rand.int[U64](x')
            y = rand.int[U64](y')
            z = rand.int[U64](z')
          end
          if ((x >= 0) and (x < x')) and ((y >= 0) and (y < y')) and ((z >= 0) and (z < z')) then
            outbounds = false
          else
            dice2 = rand.int[U64](3)
            dice1 = rand.int[U64](2)
            x = triplets(0)?
            y = triplets(1)?
            z = triplets(2)?
          end
        end
      end
      try nodePool'(x.usize())?(y.usize())?(z.usize())?.spreadRumorPushSumImp3DGrid(nodePool', env, x', y', z', (newW / 2), (newS / 2)) end
    end